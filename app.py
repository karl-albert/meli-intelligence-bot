#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
MELI INTELLIGENCE BOT - RENDER.COM WEB SERVICE (24/7 NUVEM GRATUITA)
DuckDB + Parquet (158.250 registros) + Google Gemini Flash + Telegram Webhook
========================================================================================
"""

import os
import sys
import json
import logging
import requests
import duckdb
import base64
import asyncio
import io
import re
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import google.generativeai as genai

# ==============================================================================
# 1. CONFIGURAÇÕES DE LOG E FLASK
# ==============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Render_Meli_Bot")

app = Flask(__name__)

# ==============================================================================
# 2. CREDENCIAIS E TOKENS (BASE64 E VARIÁVEIS DE AMBIENTE)
# ==============================================================================
_B64_TOK = "ODkxNjczMzY3MTpBQUgxaHR2ZDZWcURLc25nZHlZc0ZPYVhkdk5nVVEwUmp5TQ=="
_B64_GEM = "QVEuQWI4Uk42S3RWbzR3RkhZTVA4a3FiMXplWXo2dmRTLVRrakd3ZG1yY18xbzY4MURuUUE="

FALLBACK_KEY = base64.b64decode(_B64_GEM).decode("utf-8").strip()
FALLBACK_TOKEN = base64.b64decode(_B64_TOK).decode("utf-8").strip()

env_key = os.environ.get("GEMINI_KEY", "").strip()
GEMINI_KEY = env_key if (env_key and len(env_key) > 20) else FALLBACK_KEY

env_token = os.environ.get("TELEGRAM_TOKEN", "").strip()
TOKEN = env_token if (env_token and len(env_token) > 20) else FALLBACK_TOKEN

BASE_TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

# Configurar Google Gemini
genai.configure(api_key=GEMINI_KEY)
AVAILABLE_MODELS = [
    "gemini-3.1-flash-lite",
    "gemini-3.8-flash",
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-flash-latest"
]

SYNC_SECRET = os.environ.get("SYNC_SECRET", "meli_joca_sync_2026_karl")

# ==============================================================================
# 3. BANCO DE DADOS (DUCKDB + PARQUET)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILE = os.path.join(BASE_DIR, "Fato_MercadoLivre_MaisVendidos.parquet").replace("\\", "/")
con = duckdb.connect()

data_recente = None
total_registros = 0
data_inicio = None

def recarregar_duckdb():
    global con, data_recente, total_registros, data_inicio
    logger.info(f"Carregando {PARQUET_FILE} no DuckDB...")
    con.execute("DROP TABLE IF EXISTS fato_ml")
    con.execute(f"""
        CREATE TABLE fato_ml AS 
        SELECT 
            data, 
            ano, 
            mes, 
            ano_mes,
            posicao_ranking,
            categoria, 
            subcategoria, 
            titulo_produto, 
            marca, 
            TRY_CAST(qtd_vendas_estimadas_dia AS INT) as qtd_vendas_num, 
            TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE) as fat_num, 
            TRY_CAST(REPLACE(REPLACE(preco_atual, '.', ''), ',', '.') AS DOUBLE) as preco_num, 
            is_full, 
            frete_gratis 
        FROM '{PARQUET_FILE}'
    """)
    data_recente = con.execute("SELECT MAX(data) FROM fato_ml").fetchone()[0]
    total_registros = con.execute("SELECT COUNT(*) FROM fato_ml").fetchone()[0]
    data_inicio = con.execute("SELECT MIN(data) FROM fato_ml").fetchone()[0]
    logger.info(f"[OK] Base DuckDB pronta: {total_registros:,} registros. Período: {data_inicio} até {data_recente}")

recarregar_duckdb()


# ==============================================================================
# 4. FUNÇÕES DE SUPORTE A IA (GEMINI)
# ==============================================================================
def chamar_gemini(prompt):
    for model_name in AVAILABLE_MODELS:
        try:
            m = genai.GenerativeModel(model_name)
            resp = m.generate_content(prompt)
            if resp and resp.text:
                return resp.text.strip()
        except Exception as e:
            err_msg = str(e)
            logger.error(f"Erro Gemini {model_name}: {err_msg}")
            if "401" in err_msg or "Unauthenticated" in err_msg or "invalid authentication" in err_msg.lower():
                try:
                    logger.info("Reconfigurando com chave garantida...")
                    genai.configure(api_key=FALLBACK_KEY)
                    m = genai.GenerativeModel(model_name)
                    resp = m.generate_content(prompt)
                    if resp and resp.text:
                        return resp.text.strip()
                except Exception as e2:
                    logger.error(f"Erro fallback: {e2}")
            elif "429" in err_msg or "ResourceExhausted" in err_msg:
                logger.info(f"Cota 429 em {model_name}, alternando...")
                continue
            elif "404" in err_msg or "NotFound" in err_msg:
                continue
            else:
                continue
    return None


def transcrever_audio(audio_bytes, mime_type="audio/ogg"):
    """Transcreve mensagem de voz do Telegram usando Google Gemini Multimodal"""
    clean_mime = mime_type.split(";")[0].strip()
    prompt = "Transcreva com máxima fidelidade o que foi falado neste áudio em português do Brasil. Retorne APENAS o texto falado, sem introduções, sem aspas e sem explicações."
    part = {"mime_type": clean_mime, "data": audio_bytes}
    for model_name in AVAILABLE_MODELS:
        try:
            m = genai.GenerativeModel(model_name)
            resp = m.generate_content([part, prompt])
            if resp and resp.text:
                texto_limpo = resp.text.strip().replace('"', '').replace("'", "")
                logger.info(f"Áudio transcrito via {model_name}: {texto_limpo}")
                return texto_limpo
        except Exception as e:
            logger.warning(f"Tentativa transcrição áudio {model_name} falhou: {e}")
            continue
    return None


def gerar_roteiro_fala(texto_resposta, user_name):
    """Cria fala natural executiva estruturada para síntese em áudio"""
    prompt_fala = f"""
Você é o assistente virtual executivo Joca do Mercado Livre.
Abaixo está a resposta em texto formatado para o Telegram:
{texto_resposta}

Crie um roteiro de fala conciso (de 10 a 15 segundos, no máximo 3 frases) para você falar em uma nota de voz para {user_name}.
Regras obrigatórias:
- Comece de forma amigável e dinâmica: "Fala {user_name}!..."
- NÃO use asteriscos, hashtags, sublinhados, links, emojis ou marcadores de lista.
- Diga valores monetários e números por extenso de forma falada natural.
- Retorne APENAS o texto a ser falado.
"""
    fala = chamar_gemini(prompt_fala)
    if not fala:
        fala = f"Fala {user_name}! Finalizei sua consulta com sucesso. Os dados completos já estão na sua tela."
    
    for c in ["*", "#", "_", "`", "~", "[", "]", "(", ")", ">", "<"]:
        fala = fala.replace(c, "")
    return fala.strip()


# ==============================================================================
# 5. COMUNICAÇÃO COM O TELEGRAM (MENSAGENS E VOZ)
# ==============================================================================
def enviar_mensagem(chat_id, texto):
    try:
        url = f"{BASE_TELEGRAM_URL}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": texto,
            "parse_mode": "Markdown"
        }
        res = requests.post(url, json=payload, timeout=10)
        data = res.json()
        if not data.get("ok"):
            payload.pop("parse_mode", None)
            res = requests.post(url, json=payload, timeout=10)
        return True
    except Exception as e:
        logger.error(f"Erro ao enviar para Telegram: {e}")
        return False


async def _sintetizar_edge(texto):
    import edge_tts
    comm = edge_tts.Communicate(texto, "pt-BR-AntonioNeural")
    chunks = bytearray()
    async for c in comm.stream():
        if c["type"] == "audio":
            chunks.extend(c["data"])
    return bytes(chunks)


def enviar_voz(chat_id, texto_fala):
    """Sintetiza e envia áudio via Edge-TTS (Masculino) ou gTTS (Fallback)"""
    try:
        audio_data = None
        try:
            audio_data = asyncio.run(_sintetizar_edge(texto_fala))
            logger.info("Voz sintetizada com sucesso via Edge-TTS (Antonio Neural Masculino)")
        except Exception as e_edge:
            logger.error(f"FALHA NO EDGE-TTS: {e_edge}. Usando fallback gTTS...")

        if not audio_data:
            from gtts import gTTS
            tts = gTTS(text=texto_fala, lang="pt", tld="com.br")
            fp = io.BytesIO()
            tts.write_to_fp(fp)
            fp.seek(0)
            audio_data = fp.getvalue()

        url_voice = f"{BASE_TELEGRAM_URL}/sendVoice"
        files_voice = {"voice": ("joca_voz.mp3", audio_data, "audio/mpeg")}
        res_v = requests.post(url_voice, data={"chat_id": chat_id}, files=files_voice, timeout=25)
        if res_v.status_code == 200 and res_v.json().get("ok"):
            logger.info(f"Nota de voz enviada com sucesso para chat {chat_id}")
            return True
            
        url_audio = f"{BASE_TELEGRAM_URL}/sendAudio"
        files_audio = {"audio": ("joca_audio.mp3", audio_data, "audio/mpeg")}
        res_a = requests.post(
            url_audio, 
            data={"chat_id": chat_id, "title": "Joca Responde", "performer": "Joca Meli Bot"}, 
            files=files_audio, 
            timeout=25
        )
        return res_a.status_code == 200 and res_a.json().get("ok")
    except Exception as e:
        logger.error(f"Erro ao sintetizar/enviar áudio para Telegram: {e}")
        return False


# ==============================================================================
# 6. FORMATAÇÃO E PROCESSAMENTO DE PERGUNTAS (TEXT-TO-SQL)
# ==============================================================================
def formatar_resultado_python(col_names, rows, user_name, pergunta_usuario):
    if not rows:
        return f"Fala {user_name}! Não encontrei registros na base oficial para a sua pergunta."
    
    linhas = [f"📊 *Olá {user_name}! Pesquisei aqui vejamos o resultado:*\n"]
    linhas.append(f"🔍 _\"{pergunta_usuario}\"_\n")
    
    if len(rows) == 1 and len(col_names) == 1:
        col = col_names[0]
        val = rows[0][0]
        val_fmt = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if any(k in col.lower() for k in ["fat", "preco", "ticket", "receita"]) and isinstance(val, (int, float)) else f"{val:,.0f}".replace(",", ".") if isinstance(val, (int, float)) else str(val or "0")
        col_nome = col.replace("_", " ").title()
        linhas.append(f"📦 *{col_nome}:* `{val_fmt}`\n")
    else:
        for row in rows[:8]:
            itens = []
            for col, val in zip(col_names, row):
                v_str = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if any(k in col.lower() for k in ["fat", "preco"]) and isinstance(val, float) else f"{val:,.0f}".replace(",", ".") if isinstance(val, (int, float)) else str(val or "-")
                itens.append(f"*{col.replace('_', ' ').title()}:* {v_str}")
            linhas.append("• " + " | ".join(itens))
        
        if len(rows) > 8:
            linhas.append(f"\n_... e mais {len(rows) - 8} registros encontrados._")

    linhas.append(f"\n📌 _Dados da base do ML (Power BI) · Atualizado até {data_recente}_")
    return "\n".join(linhas)


def processar_pergunta(texto_msg, user_name):
    t_lower = texto_msg.lower().strip()
    
    # 1. Comandos de Saudação e Ajudas Rápidas
    if t_lower in ['/start', '/ajuda', 'oi', 'ola', 'olá', 'start']:
        return (
            f"👋 *Olá {user_name}! Eu sou o Meli Intelligence Bot (Render 24/7).*\n\n"
            f"Estou com a IA do **Google Gemini** integrada à base oficial de Mais Vendidos do Mercado Livre.\n"
            f"📅 *Base atualizada até:* `{data_recente}` ({total_registros:,} registros sincronizados).\n\n"
            f"🎙️ *Modo Voz Ativo:* Você pode mandar **mensagem de voz / áudio** no Telegram que eu compreendo e te respondo falando!\n\n"
            f"💡 *Exemplos de perguntas:*\n"
            f"• _\"Qual a quantidade de vendas total até agora no MTD?\"_\n"
            f"• _\"Quantos produtos no total da Apple venderam hoje?\"_\n"
            f"• _\"Qual o faturamento total de hoje?\"_\n"
        )
    
    if 'slide' in t_lower and ('exemplo' in t_lower or 'caso' in t_lower):
        return (
            f"🤖 *Meli Intelligence Bot* · _Caso de Uso do Slide 5_\n"
            f"No caso de uso do Slide 5 (dia 18/09/2026):\n\n"
            f"📦 *Volume Vendido Apple:* 40 unidades\n"
            f"💰 *Faturamento Estimado:* R$ 535.680\n"
            f"🏷️ *Preço Médio:* R$ 13.392,01  ·  *% FULL:* 151%\n"
            f"🏆 *Top 1 Anúncio:* iPhone 18 PRO MAX 512GB (10 unidades)\n\n"
            f"📌 _Hoje a base já está atualizada com dados em tempo real até {data_recente}!_"
        )

    # 2. Detecção Inteligente de Datas e Atalhos SQL
    clean_sql = None
    data_alvo = None
    m_iso = re.search(r'\b(202[5-9])-(\d{2})-(\d{2})\b', t_lower)
    m_br = re.search(r'\b(\d{1,2})[/.-](\d{1,2})(?:[/.-](202[5-9]))?\b', t_lower)
    
    if m_iso:
        data_alvo = m_iso.group(0)
    elif m_br:
        d, m, y = m_br.groups()
        data_alvo = f"{y if y else '2026'}-{int(m):02d}-{int(d):02d}"
    elif "ontem" in t_lower:
        try:
            dt_obj = datetime.strptime(str(data_recente), "%Y-%m-%d")
            data_alvo = (dt_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception:
            data_alvo = "2026-09-25"
    elif any(w in t_lower for w in ["hoje", "atual", "recente", "último dia", "ultimo dia", "dia mais recente"]):
        data_alvo = str(data_recente)

    if "data" in t_lower and ("recente" in t_lower or "ultima" in t_lower or "atualizada" in t_lower or "base" in t_lower) and not (m_iso or m_br):
        clean_sql = f"SELECT '{data_recente}' AS data_mais_recente, COUNT(*) AS total_registros FROM fato_ml"
    elif data_alvo and any(w in t_lower for w in ["venda", "fatur", "quanto", "número", "numero", "resultado", "pedidos", "volume"]):
        d_br = f"{data_alvo[8:10]}/{data_alvo[5:7]}/{data_alvo[:4]}"
        if "apple" in t_lower:
            clean_sql = f"SELECT '{d_br}' AS data_referencia, SUM(qtd_vendas_num) AS vendas_apple, SUM(fat_num) AS faturamento_apple FROM fato_ml WHERE data = '{data_alvo}' AND marca ILIKE '%Apple%'"
        elif "samsung" in t_lower:
            clean_sql = f"SELECT '{d_br}' AS data_referencia, SUM(qtd_vendas_num) AS vendas_samsung, SUM(fat_num) AS faturamento_samsung FROM fato_ml WHERE data = '{data_alvo}' AND marca ILIKE '%Samsung%'"
        else:
            clean_sql = f"SELECT '{d_br}' AS data_referencia, SUM(qtd_vendas_num) AS total_vendas, SUM(fat_num) AS faturamento_total FROM fato_ml WHERE data = '{data_alvo}'"
    elif "faturamento total" in t_lower and not data_alvo:
        clean_sql = "SELECT SUM(fat_num) AS faturamento_total, SUM(qtd_vendas_num) AS total_vendas FROM fato_ml"
    elif "vendas total" in t_lower and not data_alvo:
        clean_sql = "SELECT SUM(qtd_vendas_num) AS total_vendas, SUM(fat_num) AS faturamento_total FROM fato_ml"

    # 3. Text-to-SQL via Gemini + Execução no DuckDB
    try:
        dt_obj = datetime.strptime(str(data_recente), "%Y-%m-%d")
        ano_atual, mes_atual = dt_obj.year, dt_obj.month
        ontem_str = (dt_obj - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        ano_atual, mes_atual, ontem_str = 2026, 9, "2026-09-25"

    prompt_sql = f"""
Você é o motor analítico SQL DuckDB do Mercado Livre Brasil. Tabela: 'fato_ml'.
Schema: data (DATE), ano (INT), mes (INT), categoria (VARCHAR), subcategoria (VARCHAR), titulo_produto (VARCHAR), marca (VARCHAR), qtd_vendas_num (INT), fat_num (DOUBLE), preco_num (DOUBLE), is_full (BOOLEAN), frete_gratis (BOOLEAN).
Data mais recente: '{data_recente}'. MTD: ano = {ano_atual} AND mes = {mes_atual} AND data <= '{data_recente}'. Ontem: '{ontem_str}'.
Pergunta: "{texto_msg}"
Retorne APENAS a query SQL SELECT DuckDB dentro de ```sql ... ``` ou 'NAO_SQL'.
"""
    try:
        if not clean_sql:
            resp_sql = chamar_gemini(prompt_sql)
            if not resp_sql or "NAO_SQL" in resp_sql:
                return None
            clean_sql = resp_sql.replace("```sql", "").replace("```", "").strip()

        logger.info(f"SQL a executar: {clean_sql}")
        cur = con.execute(clean_sql)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        
        if not rows:
            return f"Fala {user_name}! Não foram encontrados registros na base para sua pesquisa."

        header_str = " | ".join(col_names)
        linhas_tab = [" | ".join([str(v) if v is not None else "NULL" for v in r]) for r in rows[:15]]
        tabela_str = f"{header_str}\n" + ("-" * len(header_str)) + "\n" + "\n".join(linhas_tab)

        prompt_formatacao = f"""
Você é o assistente executivo 'Meli Intelligence Bot'. O usuário '{user_name}' perguntou: "{texto_msg}"
Resultado obtido no banco oficial:
{tabela_str}

Formate a resposta para o Telegram:
- Saudação amigável: "Fala {user_name}!..."
- Emojis comerciais e formatação em R$ ou quantidade.
- Rodapé: "📌 _Dados oficiais da base do Mercado Livre (Power BI) · Atualizado até {data_recente}_"
"""
        resp_final = chamar_gemini(prompt_formatacao)
        return resp_final if resp_final else formatar_resultado_python(col_names, rows, user_name, texto_msg)
            
    except Exception as e:
        logger.error(f"Erro IA/DuckDB: {e}")
        return formatar_resultado_python(col_names, rows, user_name, texto_msg) if ('col_names' in locals() and 'rows' in locals()) else None


# ==============================================================================
# 7. ROTAS FLASK (ENDPOINTS DA APLICAÇÃO NO RENDER)
# ==============================================================================
@app.route("/", methods=["GET"])
def home():
    return f"""
    <html>
    <head><title>Meli Intelligence Bot</title></head>
    <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f8fafc;">
        <h1 style="color: #0f172a;">🤖 Meli Intelligence Bot está ONLINE!</h1>
        <p style="font-size: 18px; color: #475569;">Rodando 24/7 na nuvem gratuita do Render.com</p>
        <div style="background: white; max-width: 500px; margin: 20px auto; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);">
            <p><strong>Status:</strong> Ativo 🟢</p>
            <p><strong>Registros Carregados:</strong> {total_registros:,}</p>
            <p><strong>Data de Referência:</strong> {data_recente}</p>
            <p><strong>Telegram:</strong> <a href="https://t.me/Joca_Meli_bot" target="_blank">@Joca_Meli_bot</a></p>
        </div>
        <p><a href="/set_webhook" style="background: #2563eb; color: white; padding: 10px 20px; border-radius: 8px; text-decoration: none;">Configurar Webhook no Telegram</a></p>
    </body>
    </html>
    """

@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status": "online",
        "total_registros": total_registros,
        "data_recente": str(data_recente),
        "bot": "@Joca_Meli_bot"
    })

@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True)
    if not payload or "message" not in payload:
        return jsonify({"status": "no payload/message"}), 200

    msg = payload["message"]
    chat_id = msg.get("chat", {}).get("id")
    user_name = msg.get("from", {}).get("first_name", "Parceiro")
    
    if not chat_id:
        return jsonify({"status": "no chat_id"}), 200

    texto, origem_audio = None, False

    # Tratamento de voz recebida
    if "voice" in msg or "audio" in msg:
        media_obj = msg.get("voice") or msg.get("audio")
        file_id = media_obj.get("file_id")
        mime_type = media_obj.get("mime_type", "audio/ogg")
        try:
            requests.post(f"{BASE_TELEGRAM_URL}/sendChatAction", json={"chat_id": chat_id, "action": "record_voice"}, timeout=5)
            get_f = requests.get(f"{BASE_TELEGRAM_URL}/getFile?file_id={file_id}", timeout=10).json()
            if get_f.get("ok"):
                f_path = get_f["result"]["file_path"]
                dl_url = f"https://api.telegram.org/file/bot{TOKEN}/{f_path}"
                audio_bytes = requests.get(dl_url, timeout=20).content
                texto = transcrever_audio(audio_bytes, mime_type)
                origem_audio = True
        except Exception as e_audio:
            logger.error(f"Erro ao processar áudio recebido: {e_audio}")
            enviar_mensagem(chat_id, "🎙️ Não consegui ouvir seu áudio com clareza. Poderia repetir ou digitar?")
            return jsonify({"status": "audio error"}), 200

    elif "text" in msg:
        texto = msg["text"]

    if not texto:
        return jsonify({"status": "no text to process"}), 200

    resposta = processar_pergunta(texto, user_name) or (
        f"🤖 *Meli Intelligence Bot*\n"
        f"Não consegui processar a consulta para: _\"{texto}\"_\n\n"
        f"Tente reformular, por exemplo:\n"
        f"• *\"Total de vendas no MTD\"*\n"
        f"• *\"Faturamento de hoje por categoria\"*"
    )

    # Envia resposta textual
    enviar_mensagem(chat_id, resposta)
    
    # Envia resposta em voz se solicitado ou se veio por áudio
    quer_audio = origem_audio or any(w in texto.lower() for w in ["áudio", "audio", "por voz", "fale", "mande áudio", "voz"])
    if quer_audio:
        try:
            requests.post(f"{BASE_TELEGRAM_URL}/sendChatAction", json={"chat_id": chat_id, "action": "record_voice"}, timeout=5)
            fala = gerar_roteiro_fala(resposta, user_name)
            enviar_voz(chat_id, fala)
        except Exception as e_voz:
            logger.error(f"Erro ao gerar/enviar voz de resposta: {e_voz}")

    return jsonify({"status": "success"}), 200

@app.route("/set_webhook", methods=["GET"])
def set_webhook():
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    webhook_url = f"{host_url}/webhook"
    res = requests.post(f"{BASE_TELEGRAM_URL}/setWebhook", json={"url": webhook_url}).json()
    return jsonify({"telegram_response": res, "webhook_url": webhook_url})

@app.route("/debug_gemini", methods=["GET"])
def debug_gemini():
    logs = []
    for model_name in AVAILABLE_MODELS:
        try:
            m = genai.GenerativeModel(model_name)
            resp = m.generate_content("Diga OK")
            logs.append(f"{model_name}: SUCESSO -> {resp.text.strip()}")
            break
        except Exception as e:
            logs.append(f"{model_name}: ERRO -> {type(e).__name__}: {str(e)}")
    return jsonify({"gemini_key_len": len(GEMINI_KEY), "models_tried": logs})

@app.route("/test_ai", methods=["GET"])
def test_ai():
    q = request.args.get("q", "Qual a data mais recente?")
    return jsonify({"pergunta": q, "resposta": processar_pergunta(q, "Karl")})

@app.route("/test_voice", methods=["GET"])
def test_voice():
    text = request.args.get("text", "Fala Karl! O Joca está com voz masculina executiva.")
    try:
        audio_data = asyncio.run(_sintetizar_edge(text))
        engine = "edge-tts: pt-BR-AntonioNeural"
    except Exception:
        from gtts import gTTS
        tts = gTTS(text=text, lang="pt", tld="com.br")
        fp = io.BytesIO()
        tts.write_to_fp(fp)
        audio_data = fp.getvalue()
        engine = "gtts fallback"
    return jsonify({"status": "success", "engine": engine, "bytes": len(audio_data)})

@app.route("/sync_data", methods=["GET", "POST"])
def sync_data():
    if (request.args.get("secret") or request.headers.get("X-Sync-Secret")) != SYNC_SECRET:
        return jsonify({"status": "error", "message": "Chave inválida."}), 403

    if request.method == "GET":
        return jsonify({
            "status": "ready",
            "total_registros": total_registros,
            "data_recente": str(data_recente)
        })

    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"status": "error", "message": "Nenhum arquivo enviado."}), 400

    try:
        request.files["file"].save(PARQUET_FILE)
        recarregar_duckdb()
        return jsonify({
            "status": "success",
            "message": "Base de dados atualizada com sucesso no DuckDB!",
            "total_registros": total_registros,
            "data_recente": str(data_recente)
        }), 200
    except Exception as e:
        logger.error(f"Erro na sincronização: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ==============================================================================
# 8. EXECUÇÃO DO APLICATIVO
# ==============================================================================
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
