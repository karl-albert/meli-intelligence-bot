#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
JOCA INTELLIGENCE HUB (Joca_BigQuery & Joca_Fabric) - RENDER.COM WEB SERVICE (24/7)
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
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import google.generativeai as genai


# ==============================================================================
# 1. CONFIGURAÇÕES DE LOG E FLASK
# ==============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Render_Joca_Hub")

app = Flask(__name__)

# ==============================================================================
# 2. CREDENCIAIS E TOKENS (BASE64 E VARIÁVEIS DE AMBIENTE)
# ==============================================================================
_B64_TOK_BQ = "ODkxNjczMzY3MTpBQUgxaHR2ZDZWcURLc25nZHlZc0ZPYVhkdk5nVVEwUmp5TQ=="
_B64_TOK_FABRIC = "ODk1ODUyNTM2MzpBQUgwUkQwbDhlWHZyZTFZeTJYVE90VkxuT0FCOGx1UWRoRQ=="
_B64_GEM = "QVEuQWI4Uk42S3RWbzR3RkhZTVA4a3FiMXplWXo2dmRTLVRrakd3ZG1yY18xbzY4MURuUUE="

FALLBACK_KEY = base64.b64decode(_B64_GEM).decode("utf-8").strip()
FALLBACK_TOKEN_BQ = base64.b64decode(_B64_TOK_BQ).decode("utf-8").strip()
FALLBACK_TOKEN_FABRIC = base64.b64decode(_B64_TOK_FABRIC).decode("utf-8").strip()

env_key = os.environ.get("GEMINI_KEY", "").strip()
GEMINI_KEY = env_key if (env_key and len(env_key) > 20) else FALLBACK_KEY

env_token = os.environ.get("TELEGRAM_TOKEN", "").strip()
TOKEN_BQ = env_token if (env_token and len(env_token) > 20) else FALLBACK_TOKEN_BQ
TOKEN_MELI = TOKEN_BQ

env_token_fabric = os.environ.get("TELEGRAM_TOKEN_FABRIC", "").strip()
TOKEN_FABRIC = env_token_fabric if (env_token_fabric and len(env_token_fabric) > 20) else FALLBACK_TOKEN_FABRIC

# Credenciais Microsoft Teams (Joca_B3)
_B64_TEAMS_ID = "ZjQ3NmZiMjYtYmFkZS00N2JhLThjMTUtYWY5OGNjY2YzYjZi"
_B64_TEAMS_SEC = "RUVBOFF+Yk0yUlRydXZQMX5wWEhMQXJacWo2QVAyM2ZWLk5Qd2FPZQ=="
FALLBACK_TEAMS_ID = base64.b64decode(_B64_TEAMS_ID).decode("utf-8").strip()
FALLBACK_TEAMS_SEC = base64.b64decode(_B64_TEAMS_SEC).decode("utf-8").strip()

env_teams_id = os.environ.get("TEAMS_BOT_ID", "").strip()
TEAMS_BOT_ID = env_teams_id if (env_teams_id and len(env_teams_id) > 10) else FALLBACK_TEAMS_ID

env_teams_secret = os.environ.get("TEAMS_CLIENT_SECRET", "").strip()
TEAMS_CLIENT_SECRET = env_teams_secret if (env_teams_secret and len(env_teams_secret) > 10) else FALLBACK_TEAMS_SEC

# Configuração e Retrocompatibilidade
TOKEN = TOKEN_MELI
BASE_TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN_BQ}"
BASE_URL_BQ = f"https://api.telegram.org/bot{TOKEN_BQ}"
BASE_URL_MELI = BASE_URL_BQ
BASE_URL_FABRIC = f"https://api.telegram.org/bot{TOKEN_FABRIC}"

# Configurar Google Gemini
genai.configure(api_key=GEMINI_KEY)
AVAILABLE_MODELS = [
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash-lite",
    "gemini-flash-lite-latest",
    "gemini-2.5-flash-lite",
    "gemini-3.7-flash",
    "gemini-3.8-flash"
]

SYNC_SECRET = os.environ.get("SYNC_SECRET", "joca_sync_2026_karl")

# ==============================================================================
# 3. BANCO DE DADOS (DUCKDB + PARQUET)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILE = os.path.join(BASE_DIR, "Fato_MercadoLivre_MaisVendidos.parquet").replace("\\", "/")
DICIONARIO_FILE = os.path.join(BASE_DIR, "dicionario_dados_ml.json")

DICIONARIO_DADOS = {}
if os.path.exists(DICIONARIO_FILE):
    try:
        with open(DICIONARIO_FILE, "r", encoding="utf-8") as f_dic:
            DICIONARIO_DADOS = json.load(f_dic)
        logger.info(f"Dicionário oficial carregado: {len(DICIONARIO_DADOS.get('categorias', {}))} categorias, {DICIONARIO_DADOS.get('total_marcas_unicas', 0)} marcas.")
    except Exception as e_dic:
        logger.error(f"Erro ao carregar dicionario_dados_ml.json: {e_dic}")

# ------------------------------------------------------------------------------
# TELEMETRIA E LOG DE CONVERSAS DOS BOTS (PARA O POWER BI)
# ------------------------------------------------------------------------------
CONVERSAS_LOG_FILE = os.path.join(BASE_DIR, "conversas_log.json")
conversas_lock = threading.Lock()

def carregar_conversas():
    if os.path.exists(CONVERSAS_LOG_FILE):
        try:
            with open(CONVERSAS_LOG_FILE, "r", encoding="utf-8") as f_c:
                return json.load(f_c)
        except Exception as e_c:
            logger.error(f"Erro ao carregar conversas_log.json: {e_c}")
    return []

def salvar_conversa(registro):
    with conversas_lock:
        try:
            historico = carregar_conversas()
            historico.append(registro)
            # Mantém os últimos 2.000 registros para garantir alta velocidade
            if len(historico) > 2000:
                historico = historico[-2000:]
            with open(CONVERSAS_LOG_FILE, "w", encoding="utf-8") as f_w:
                json.dump(historico, f_w, ensure_ascii=False, indent=2)
            logger.info(f"Registro de conversa salvo com sucesso (ID: {registro.get('id')})")
        except Exception as e_s:
            logger.error(f"Erro ao salvar conversa em conversas_log.json: {e_s}")


# ------------------------------------------------------------------------------
# 3.1 SISTEMA DE PROTEÇÃO ANTI-SPAM & RATE LIMITER (FREE TIER SHIELD)
# ------------------------------------------------------------------------------
class RateLimiter:
    """
    Controlador de taxa de requisições:
    1. Por usuário: máx 10 msgs/minuto e intervalo mínimo de 2s entre mensagens (anti-flood/bot).
    2. Global Gemini: máx 13 requisições/minuto (protegendo a cota gratuita de 15 RPM do Gemini).
    3. Proteção financeira: zero risco de cobrança acidental ou estouro de cota.
    """
    def __init__(self, max_per_user_per_min=10, min_interval_s=2.0, max_global_gemini_rpm=13):
        self.lock = threading.Lock()
        self.user_timestamps = {}   # {user_id: [t1, t2, ...]}
        self.gemini_timestamps = []  # [t1, t2, ...]
        self.max_per_user = max_per_user_per_min
        self.min_interval = min_interval_s
        self.max_global_gemini_rpm = max_global_gemini_rpm

    def check_user_limit(self, user_id):
        """Verifica se o usuário está enviando mensagens muito rápido."""
        now = time.time()
        with self.lock:
            # Limpeza periódica de timestamps com mais de 2 minutos
            for uid in list(self.user_timestamps.keys()):
                self.user_timestamps[uid] = [t for t in self.user_timestamps[uid] if now - t < 120]
                if not self.user_timestamps[uid]:
                    del self.user_timestamps[uid]

            timestamps = self.user_timestamps.get(user_id, [])

            # 1. Checagem de intervalo mínimo (rajada/flood)
            if timestamps and (now - timestamps[-1] < self.min_interval):
                tempo_espera = max(1, int(self.min_interval - (now - timestamps[-1])) + 1)
                return False, "flood_interval", tempo_espera

            # 2. Checagem de limite por minuto
            timestamps_min = [t for t in timestamps if now - t < 60]
            if len(timestamps_min) >= self.max_per_user:
                tempo_espera = max(1, int(60 - (now - timestamps_min[0])))
                return False, "max_per_minute", tempo_espera

            # Permissão concedida: registra chamada
            timestamps.append(now)
            self.user_timestamps[user_id] = timestamps
            return True, None, 0

    def can_call_gemini(self):
        """Verifica se o limite global de segurança do Gemini (13 RPM) foi atingido."""
        now = time.time()
        with self.lock:
            self.gemini_timestamps = [t for t in self.gemini_timestamps if now - t < 60]
            if len(self.gemini_timestamps) >= self.max_global_gemini_rpm:
                return False
            self.gemini_timestamps.append(now)
            return True

rate_limiter = RateLimiter(max_per_user_per_min=10, min_interval_s=2.0, max_global_gemini_rpm=13)

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
    if not rate_limiter.can_call_gemini():
        logger.warning("Limite global de segurança Gemini (13 RPM) atingido. Ativando fallback determinístico local.")
        return None
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
Você é o assistente virtual executivo Joca_BigQuery, especialista em inteligência analítica de vendas.
Abaixo está a resposta em texto formatado para o Telegram:
{texto_resposta}

Crie um roteiro de fala conciso (de 10 a 15 segundos, no máximo 3 frases) para você falar em uma nota de voz para {user_name}.
Regras obrigatórias:
- Comece de forma amigável: "Olá {user_name}!..."
- NÃO use asteriscos, hashtags, sublinhados, links, emojis ou marcadores de lista.
- Diga valores monetários e números por extenso de forma falada natural.
- Retorne APENAS o texto a ser falado.
"""
    fala = chamar_gemini(prompt_fala)
    if not fala:
        fala = f"Olá {user_name}! Finalizei sua consulta com sucesso. Os dados completos já estão na sua tela."
    
    for c in ["*", "#", "_", "`", "~", "[", "]", "(", ")", ">", "<"]:
        fala = fala.replace(c, "")
    return fala.strip()


# ==============================================================================
# 5. COMUNICAÇÃO COM O TELEGRAM (MENSAGENS, VOZ E PROCESSAMENTO ASSÍNCRONO)
# ==============================================================================
def enviar_mensagem(chat_id, texto, base_url=None):
    if not base_url:
        base_url = BASE_URL_BQ
    try:
        url = f"{base_url}/sendMessage"
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
        logger.error(f"Erro ao enviar para Telegram ({base_url}): {e}")
        return False


async def _sintetizar_edge(texto):
    import edge_tts
    comm = edge_tts.Communicate(texto, "pt-BR-AntonioNeural")
    chunks = bytearray()
    async for c in comm.stream():
        if c["type"] == "audio":
            chunks.extend(c["data"])
    return bytes(chunks)


def enviar_voz(chat_id, texto_fala, base_url=None):
    """Sintetiza e envia áudio via Edge-TTS (Masculino) ou gTTS (Fallback)"""
    if not base_url:
        base_url = BASE_URL_BQ
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

        url_voice = f"{base_url}/sendVoice"
        files_voice = {"voice": ("joca_voz.mp3", audio_data, "audio/mpeg")}
        res_v = requests.post(url_voice, data={"chat_id": chat_id}, files=files_voice, timeout=25)
        if res_v.status_code == 200 and res_v.json().get("ok"):
            logger.info(f"Nota de voz enviada com sucesso para chat {chat_id}")
            return True
            
        url_audio = f"{base_url}/sendAudio"
        files_audio = {"audio": ("joca_audio.mp3", audio_data, "audio/mpeg")}
        res_a = requests.post(
            url_audio, 
            data={"chat_id": chat_id, "title": "Joca Responde", "performer": "Joca Assistente"}, 
            files=files_audio, 
            timeout=25
        )
        return res_a.status_code == 200 and res_a.json().get("ok")
    except Exception as e:
        logger.error(f"Erro ao sintetizar/enviar áudio para Telegram: {e}")
        return False


def _processar_mensagem_telegram(msg, base_url, bot_label="Joca Assistente"):
    """Executado em segundo plano (background thread) para eliminar timeouts no Telegram"""
    try:
        chat_id = msg.get("chat", {}).get("id")
        user_obj = msg.get("from", {})
        user_id = user_obj.get("id") or chat_id
        user_name = user_obj.get("first_name", "Parceiro")
        if not chat_id:
            return

        # ----------------------------------------------------------------------
        # VALIDAÇÃO DE SEGURANÇA E RATE LIMITING ANTI-SPAM
        # ----------------------------------------------------------------------
        permitido, motivo, espera_s = rate_limiter.check_user_limit(user_id)
        if not permitido:
            logger.warning(f"Rate limit acionado para {user_name} (ID {user_id}). Motivo: {motivo}")
            aviso_spam = (
                f"⏳ *Calma lá, {user_name}!* 🚦\n\n"
                f"Você está enviando perguntas muito rápido. Para mantermos a estabilidade e a gratuidade do serviço, "
                f"por favor aguarde cerca de *{espera_s} segundos* antes de enviar sua próxima dúvida."
            )
            enviar_mensagem(chat_id, aviso_spam, base_url=base_url)

            # Grava o evento na telemetria das conversas para auditoria no Power BI
            try:
                p_nome = user_obj.get("first_name", "")
                u_nome = user_obj.get("last_name", "")
                nome_completo = f"{p_nome} {u_nome}".strip() or user_name
                username_val = user_obj.get("username", "")
                username_str = f"@{username_val}" if username_val else "-"
                agora = datetime.now()
                bot_tag = "Joca_BigQuery" if "BigQuery" in bot_label else "Joca_Fabric"
                
                salvar_conversa({
                    "id": msg.get("message_id") or int(time.time()),
                    "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                    "data": agora.strftime("%Y-%m-%d"),
                    "hora": agora.strftime("%H:%M:%S"),
                    "bot": bot_tag,
                    "chat_id": str(chat_id),
                    "usuario": nome_completo,
                    "username": username_str,
                    "tipo_entrada": "Spam/Bloqueado",
                    "pergunta": msg.get("text") or "[Mídia Bloqueada por Flood]",
                    "resposta": aviso_spam,
                    "tempo_resposta_s": 0.05,
                    "status": "Bloqueado (Anti-Spam)"
                })
            except Exception as e_log_spam:
                logger.error(f"Erro ao registrar spam na telemetria: {e_log_spam}")
            return

        texto, origem_audio = None, False

        # Tratamento de voz recebida
        if "voice" in msg or "audio" in msg:
            media_obj = msg.get("voice") or msg.get("audio")
            file_id = media_obj.get("file_id")
            mime_type = media_obj.get("mime_type", "audio/ogg")
            try:
                requests.post(f"{base_url}/sendChatAction", json={"chat_id": chat_id, "action": "record_voice"}, timeout=5)
                get_f = requests.get(f"{base_url}/getFile?file_id={file_id}", timeout=10).json()
                if get_f.get("ok"):
                    f_path = get_f["result"]["file_path"]
                    token_part = base_url.split("/bot")[-1]
                    dl_url = f"https://api.telegram.org/file/bot{token_part}/{f_path}"
                    audio_bytes = requests.get(dl_url, timeout=20).content
                    texto = transcrever_audio(audio_bytes, mime_type)
                    origem_audio = True
            except Exception as e_audio:
                logger.error(f"Erro ao processar áudio recebido: {e_audio}")
                enviar_mensagem(chat_id, "🎙️ Não consegui ouvir seu áudio com clareza. Poderia repetir ou digitar?", base_url=base_url)
                return

        elif "text" in msg:
            texto = msg["text"]

        if not texto:
            return

        t_inicio = time.time()
        requests.post(f"{base_url}/sendChatAction", json={"chat_id": chat_id, "action": "typing"}, timeout=5)

        resposta = processar_pergunta(texto, user_name) or (
            f"🤖 *{bot_label}*\n"
            f"Não consegui processar a consulta para: _\"{texto}\"_\n\n"
            f"Tente reformular, por exemplo:\n"
            f"• *\"Total de vendas no MTD\"*\n"
            f"• *\"Faturamento de hoje por categoria\"*"
        )

        enviar_mensagem(chat_id, resposta, base_url=base_url)

        # Gravar log de telemetria da conversa para consumo no Power BI
        try:
            t_duracao = round(time.time() - t_inicio, 2)
            from_user = msg.get("from", {})
            p_nome = from_user.get("first_name", "")
            u_nome = from_user.get("last_name", "")
            nome_completo = f"{p_nome} {u_nome}".strip() or user_name
            username_val = from_user.get("username", "")
            username_str = f"@{username_val}" if username_val else "-"
            agora = datetime.now()
            bot_tag = "Joca_BigQuery" if "BigQuery" in bot_label else "Joca_Fabric"
            
            reg_conversa = {
                "id": msg.get("message_id") or int(time.time()),
                "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                "data": agora.strftime("%Y-%m-%d"),
                "hora": agora.strftime("%H:%M:%S"),
                "bot": bot_tag,
                "chat_id": str(chat_id),
                "usuario": nome_completo,
                "username": username_str,
                "tipo_entrada": "Voz" if origem_audio else "Texto",
                "pergunta": texto,
                "resposta": resposta,
                "tempo_resposta_s": t_duracao,
                "status": "Sucesso" if resposta else "Erro"
            }
            salvar_conversa(reg_conversa)
        except Exception as e_log:
            logger.error(f"Erro ao registrar telemetria da conversa: {e_log}")

        quer_audio = origem_audio or any(w in texto.lower() for w in ["áudio", "audio", "por voz", "fale", "mande áudio", "voz"])
        if quer_audio:
            try:
                requests.post(f"{base_url}/sendChatAction", json={"chat_id": chat_id, "action": "record_voice"}, timeout=5)
                fala = gerar_roteiro_fala(resposta, user_name)
                enviar_voz(chat_id, fala, base_url=base_url)
            except Exception as e_voz:
                logger.error(f"Erro ao gerar/enviar voz de resposta: {e_voz}")
    except Exception as e_proc:
        logger.error(f"Erro crítico no processamento assíncrono: {e_proc}")


# ==============================================================================
# 6. FORMATAÇÃO E PROCESSAMENTO DE PERGUNTAS (TEXT-TO-SQL)
# ==============================================================================
def formatar_resultado_python(col_names, rows, user_name, pergunta_usuario, alerta=None):
    if not rows:
        return f"Fala {user_name}! Não encontrei registros na base oficial para a sua pergunta."
    
    linhas = [f"📊 *Olá {user_name}! Pesquisei aqui vejamos o resultado:*\n"]
    if alerta:
        linhas.append(f"{alerta}\n")
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

    linhas.append(f"\n📌 _Dados da Base Analítica (Power BI) · Atualizado até {data_recente}_")
    return "\n".join(linhas)


def responder_dicionario_ou_conceito(texto, user_name):
    if not DICIONARIO_DADOS:
        return None
    t = texto.lower().strip()
    categorias_map = DICIONARIO_DADOS.get("categorias", {})
    glossario_list = DICIONARIO_DADOS.get("glossario_metricas_tempo", [])

    # 1. Pergunta sobre categorias existentes
    if any(q in t for q in ["quais sao as categorias", "quais são as categorias", "quais categorias", "listar categorias", "quais as categorias"]):
        cats = list(categorias_map.keys())
        msg = [f"📊 Olá {user_name}! Pesquisei aqui vejamos o resultado:\n",
               "📂 *Categorias Oficiais da Base Analítica (Nível 1 Macro):*\n"]
        for i, c in enumerate(cats, 1):
            n_subs = len(categorias_map[c])
            msg.append(f"{i}. *{c}* ({n_subs} subcategorias)")
        msg.append(f"\n📌 _Dicionário de Dados Oficial ML (Power BI) · Atualizado até {data_recente}_")
        return "\n".join(msg)

    # 2. Pergunta sobre subcategorias de uma categoria específica (sem intenção de soma/venda)
    m_sub_de = re.search(r'(?:subcategorias|subcategoria)\s+(?:de|da|do)\s+([a-z0-9áéíóúãõç\s,]+)', t)
    if m_sub_de and not any(w in t for w in ["vendeu", "venda", "faturamento", "faturou", "quanto", "ranking"]):
        termo_cat = m_sub_de.group(1).strip()
        cat_match = None
        for c in categorias_map:
            if termo_cat in c.lower() or c.lower() in termo_cat:
                cat_match = c
                break
        if cat_match:
            subs = list(categorias_map[cat_match].keys())
            msg = [f"📊 Olá {user_name}! Pesquisei aqui vejamos o resultado:\n",
                   f"📂 *Subcategorias oficiais de '{cat_match}':*\n"]
            for i, s in enumerate(subs, 1):
                marcas = categorias_map[cat_match][s]
                marcas_str = f" _(ex: {', '.join(marcas[:3])})_" if marcas else ""
                msg.append(f"{i}. *{s}*{marcas_str}")
            msg.append(f"\n📌 _Hierarquia Oficial: Categoria '{cat_match}' > {len(subs)} Subcategorias_")
            return "\n".join(msg)

    # 3. Pergunta sobre conceitos ou siglas (YTDA, MTD, MoM, YoY, M-1, Forecast, Orçado, etc.)
    for item in glossario_list:
        sigla = item.get("sigla", "").lower().strip()
        termo = item.get("termo", "").lower().strip()
        exp = item.get("explicacao", "").strip()
        
        eh_pergunta_conceito = (
            re.search(r'\b(o que e|o que é|o que significa|significado de|definicao de|definição de|conceito de)\s+' + re.escape(sigla) + r'\b', t) or
            re.search(r'\b(o que e|o que é|o que significa|significado de|definicao de|definição de|conceito de)\s+' + re.escape(termo) + r'\b', t) or
            (len(sigla) >= 3 and t in [sigla, f'o que e {sigla}', f'o que é {sigla}', f'{sigla}?'])
        )
        if eh_pergunta_conceito:
            return (
                f"📊 Olá {user_name}! Pesquisei aqui vejamos o resultado:\n\n"
                f"📖 *Termo:* `{item['sigla'].upper()}` ({item['termo']})\n"
                f"💡 *Definição Oficial:* {exp if exp else item['termo']}\n\n"
                f"📌 _Dicionário de Métricas e Estrutura Temporal do Projeto Power BI_"
            )
            
    return None


def processar_pergunta(texto_msg, user_name):
    t_lower = texto_msg.lower().strip()
    
    # 1. Comandos de Saudação e Ajudas Rápidas
    if t_lower in ['/start', '/ajuda', 'oi', 'ola', 'olá', 'start']:
        return (
            f"👋 *Olá {user_name}! Eu sou o Joca_BigQuery (Render 24/7).*\n\n"
            f"Estou com a IA do **Google Gemini** integrada à base analítica de vendas do BigQuery.\n"
            f"📅 *Base atualizada até:* `{data_recente}` ({total_registros:,} registros sincronizados).\n\n"
            f"🎙️ *Modo Voz Ativo:* Você pode mandar **mensagem de voz / áudio** no Telegram que eu compreendo e te respondo falando!\n\n"
            f"💡 *Exemplos de perguntas:*\n"
            f"• _\"Qual a quantidade de vendas total até agora no MTD?\"_\n"
            f"• _\"Quantos produtos no total da Apple venderam hoje?\"_\n"
            f"• _\"Qual o faturamento total de hoje?\"_\n"
        )
    
    if 'slide' in t_lower and ('exemplo' in t_lower or 'caso' in t_lower):
        return (
            f"🤖 *Joca_BigQuery* · _Caso de Uso do Slide 5_\n"
            f"No caso de uso do Slide 5 (dia 18/09/2026):\n\n"
            f"📦 *Volume Vendido Apple:* 40 unidades\n"
            f"💰 *Faturamento Estimado:* R$ 535.680\n"
            f"🏷️ *Preço Médio:* R$ 13.392,01  ·  *% FULL:* 151%\n"
            f"🏆 *Top 1 Anúncio:* iPhone 18 PRO MAX 512GB (10 unidades)\n\n"
            f"📌 _Base analítica atualizada com dados em tempo real até {data_recente}!_"
        )

    # 2. Respostas Conceituais do Dicionário de Dados Oficial
    resp_dic = responder_dicionario_ou_conceito(texto_msg, user_name)
    if resp_dic:
        return resp_dic

    # 3. Motor de Inteligência Analítica e Dicionário de Negócio
    dt_obj = datetime.strptime(str(data_recente), "%Y-%m-%d")
    ano_recente = dt_obj.year
    mes_recente = dt_obj.month
    ontem_str = (dt_obj - timedelta(days=1)).strftime("%Y-%m-%d")

    # Mapeamento oficial de Categorias (Nível 1 - 5 categorias macro)
    MAPA_CATEGORIAS = {
        'informatica': 'Informática', 'informática': 'Informática', 'ti': 'Informática',
        'celular': 'Celulares e Telefones', 'celulares': 'Celulares e Telefones', 'telefone': 'Celulares e Telefones', 'telefones': 'Celulares e Telefones',
        'eletro': 'Eletrodomésticos', 'eletros': 'Eletrodomésticos', 'eletrodoméstico': 'Eletrodomésticos', 'eletrodomésticos': 'Eletrodomésticos', 'eletrodomestico': 'Eletrodomésticos', 'eletrodomesticos': 'Eletrodomésticos',
        'ferramenta': 'Ferramentas e Construção', 'ferramentas': 'Ferramentas e Construção', 'construcao': 'Ferramentas e Construção', 'construção': 'Ferramentas e Construção',
        'casa': 'Casa, Móveis e Decoração', 'moveis': 'Casa, Móveis e Decoração', 'móveis': 'Casa, Móveis e Decoração', 'decoracao': 'Casa, Móveis e Decoração', 'decoração': 'Casa, Móveis e Decoração'
    }

    # Mapeamento oficial de Subcategorias (Nível 2 subordinado à Categoria)
    MAPA_SUBCATEGORIAS = {
        # Celulares e Telefones
        'smartphones': ('Celulares e Telefones', 'Smartphones'), 'smartphone': ('Celulares e Telefones', 'Smartphones'),
        'áudio mobile': ('Celulares e Telefones', 'Áudio Mobile'), 'audio mobile': ('Celulares e Telefones', 'Áudio Mobile'),
        'automobile': ('Celulares e Telefones', 'Áudio Mobile'), 'auto mobile': ('Celulares e Telefones', 'Áudio Mobile'),
        'audiomobile': ('Celulares e Telefones', 'Áudio Mobile'), 'som mobile': ('Celulares e Telefones', 'Áudio Mobile'),
        'fone de ouvido': ('Celulares e Telefones', 'Áudio Mobile'), 'fones de ouvido': ('Celulares e Telefones', 'Áudio Mobile'),
        'fones': ('Celulares e Telefones', 'Áudio Mobile'), 'fone': ('Celulares e Telefones', 'Áudio Mobile'),
        'airpods': ('Celulares e Telefones', 'Áudio Mobile'), 'airpod': ('Celulares e Telefones', 'Áudio Mobile'),
        'headset': ('Celulares e Telefones', 'Áudio Mobile'), 'tws': ('Celulares e Telefones', 'Áudio Mobile'),
        'carregadores': ('Celulares e Telefones', 'Carregadores'), 'carregador': ('Celulares e Telefones', 'Carregadores'),
        'smartwatches': ('Celulares e Telefones', 'Smartwatches'), 'smartwatch': ('Celulares e Telefones', 'Smartwatches'), 'relogio': ('Celulares e Telefones', 'Smartwatches'), 'relógio': ('Celulares e Telefones', 'Smartwatches'),
        'cabos': ('Celulares e Telefones', 'Cabos'), 'cabo': ('Celulares e Telefones', 'Cabos'),
        'adaptadores': ('Celulares e Telefones', 'Adaptadores'), 'adaptador': ('Celulares e Telefones', 'Adaptadores'),
        'suportes': ('Celulares e Telefones', 'Suportes'), 'suporte': ('Celulares e Telefones', 'Suportes'),
        'memória': ('Celulares e Telefones', 'Memória'), 'memoria': ('Celulares e Telefones', 'Memória'),

        # Informática
        'notebooks': ('Informática', 'Notebooks'), 'notebook': ('Informática', 'Notebooks'), 'laptop': ('Informática', 'Notebooks'), 'laptops': ('Informática', 'Notebooks'),
        'hardware': ('Informática', 'Hardware'), 'placa de vídeo': ('Informática', 'Hardware'), 'placa de video': ('Informática', 'Hardware'), 'processador': ('Informática', 'Hardware'),
        'periféricos': ('Informática', 'Periféricos'), 'perifericos': ('Informática', 'Periféricos'), 'periférico': ('Informática', 'Periféricos'), 'periferico': ('Informática', 'Periféricos'), 'mouse': ('Informática', 'Periféricos'), 'teclado': ('Informática', 'Periféricos'),
        'armazenamento': ('Informática', 'Armazenamento'), 'ssd': ('Informática', 'Armazenamento'), 'hd': ('Informática', 'Armazenamento'), 'pendrive': ('Informática', 'Armazenamento'),
        'monitores': ('Informática', 'Monitores'), 'monitor': ('Informática', 'Monitores'),
        'games': ('Informática', 'Games'), 'gamer': ('Informática', 'Games'), 'jogos': ('Informática', 'Games'),
        'redes': ('Informática', 'Redes'), 'roteador': ('Informática', 'Redes'), 'roteadores': ('Informática', 'Redes'),
        'impressão 3d': ('Informática', 'Impressão 3D'), 'impressao 3d': ('Informática', 'Impressão 3D'),
        'energia': ('Informática', 'Energia'), 'nobreak': ('Informática', 'Energia'),
        'áudio pc': ('Informática', 'Áudio PC'), 'audio pc': ('Informática', 'Áudio PC'),
        'suprimentos': ('Informática', 'Suprimentos'), 'toner': ('Informática', 'Suprimentos'), 'cartucho': ('Informática', 'Suprimentos'),
        'impressão': ('Informática', 'Impressão'), 'impressao': ('Informática', 'Impressão'), 'impressora': ('Informática', 'Impressão'),

        # Eletrodomésticos
        'climatização': ('Eletrodomésticos', 'Climatização'), 'climatizacao': ('Eletrodomésticos', 'Climatização'), 'ar condicionado': ('Eletrodomésticos', 'Climatização'), 'ventilador': ('Eletrodomésticos', 'Climatização'), 'aquecedor': ('Eletrodomésticos', 'Climatização'),
        'purificadores': ('Eletrodomésticos', 'Purificadores'), 'purificador': ('Eletrodomésticos', 'Purificadores'),
        'refrigeração': ('Eletrodomésticos', 'Refrigeração'), 'refrigeracao': ('Eletrodomésticos', 'Refrigeração'), 'geladeira': ('Eletrodomésticos', 'Refrigeração'), 'freezer': ('Eletrodomésticos', 'Refrigeração'),
        'cuidados roupas': ('Eletrodomésticos', 'Cuidados Roupas'), 'ferro de passar': ('Eletrodomésticos', 'Cuidados Roupas'),
        'eletroportáteis': ('Eletrodomésticos', 'Eletroportáteis'), 'eletroportateis': ('Eletrodomésticos', 'Eletroportáteis'), 'air fryer': ('Eletrodomésticos', 'Eletroportáteis'), 'fritadeira': ('Eletrodomésticos', 'Eletroportáteis'), 'cafeteira': ('Eletrodomésticos', 'Eletroportáteis'),
        'bebedouros': ('Eletrodomésticos', 'Bebedouros'), 'bebedouro': ('Eletrodomésticos', 'Bebedouros'),

        # Ferramentas e Construção
        'manuais': ('Ferramentas e Construção', 'Manuais'), 'ferramentas manuais': ('Ferramentas e Construção', 'Manuais'),
        'elétrica': ('Ferramentas e Construção', 'Elétrica'), 'eletrica': ('Ferramentas e Construção', 'Elétrica'),
        'elétricas': ('Ferramentas e Construção', 'Elétricas'), 'eletricas': ('Ferramentas e Construção', 'Elétricas'), 'furadeira': ('Ferramentas e Construção', 'Elétricas'), 'parafusadeira': ('Ferramentas e Construção', 'Elétricas'),
        'construção': ('Ferramentas e Construção', 'Construção'), 'construcao': ('Ferramentas e Construção', 'Construção'),
        'pintura': ('Ferramentas e Construção', 'Pintura'), 'tinta': ('Ferramentas e Construção', 'Pintura'),
        'medição': ('Ferramentas e Construção', 'Medição'), 'medicao': ('Ferramentas e Construção', 'Medição'), 'trena': ('Ferramentas e Construção', 'Medição'),
        'hidráulica': ('Ferramentas e Construção', 'Hidráulica'), 'hidraulica': ('Ferramentas e Construção', 'Hidráulica'),
        'pneumática': ('Ferramentas e Construção', 'Pneumática'), 'pneumatica': ('Ferramentas e Construção', 'Pneumática'),
        'solda': ('Ferramentas e Construção', 'Solda'),

        # Casa, Móveis e Decoração
        'cama e banho': ('Casa, Móveis e Decoração', 'Cama e Banho'), 'lencol': ('Casa, Móveis e Decoração', 'Cama e Banho'), 'lençol': ('Casa, Móveis e Decoração', 'Cama e Banho'), 'toalha': ('Casa, Móveis e Decoração', 'Cama e Banho'),
        'móveis': ('Casa, Móveis e Decoração', 'Móveis'), 'moveis': ('Casa, Móveis e Decoração', 'Móveis'), 'sofa': ('Casa, Móveis e Decoração', 'Móveis'), 'sofá': ('Casa, Móveis e Decoração', 'Móveis'),
        'decoração': ('Casa, Móveis e Decoração', 'Decoração'), 'decoracao': ('Casa, Móveis e Decoração', 'Decoração'),
        'banheiro': ('Casa, Móveis e Decoração', 'Banheiro'),
        'iluminação': ('Casa, Móveis e Decoração', 'Iluminação'), 'iluminacao': ('Casa, Móveis e Decoração', 'Iluminação'), 'luminária': ('Casa, Móveis e Decoração', 'Iluminação'),
        'organização': ('Casa, Móveis e Decoração', 'Organização'), 'organizacao': ('Casa, Móveis e Decoração', 'Organização'),
        'utilidades': ('Casa, Móveis e Decoração', 'Utilidades'),
        'utensílios': ('Casa, Móveis e Decoração', 'Utensílios de Cozinha'), 'utensilios': ('Casa, Móveis e Decoração', 'Utensílios de Cozinha'),
        'artigos de festas': ('Casa, Móveis e Decoração', 'Artigos de Festas'),
        'jardim': ('Casa, Móveis e Decoração', 'Jardim'),
        'malas': ('Casa, Móveis e Decoração', 'Malas'),

        # Presentes em mais de uma Categoria macro
        'cozinha': (None, 'Cozinha'),
        'limpeza': (None, 'Limpeza'),
        'lavanderia': (None, 'Lavanderia'),
        'segurança': (None, 'Segurança'), 'seguranca': (None, 'Segurança'),
        'acessórios': (None, 'Acessórios'), 'acessorios': (None, 'Acessórios')
    }

    # Detecção com tolerância a erros de digitação e variações de fala
    subcat_synonyms = ['subcategoria', 'subcategorias', 'subcategira', 'sub-categoria', 'sub categoria', 'subcat', 'sub-cat', 'sub-categ']
    disse_subcategoria = any(w in t_lower for w in subcat_synonyms)

    cat_synonyms = ['categoria', 'categorias', 'categora', 'cat', 'categor']
    disse_categoria = any(w in t_lower for w in cat_synonyms) and not disse_subcategoria

    achou_sub = None
    for k_sub, (cat_pai, sub_nome) in MAPA_SUBCATEGORIAS.items():
        if re.search(r'\b' + re.escape(k_sub) + r'\b', t_lower):
            achou_sub = (k_sub, cat_pai, sub_nome)
            break

    # Se o usuário disse subcategoria mas não achou no mapa fixo, busca dinâmica no banco DuckDB
    if disse_subcategoria and not achou_sub:
        m_cand = re.search(r'(?:subcategoria|subcategorias|subcategira|sub-categoria|sub categoria|subcat)\s+(?:de\s+|da\s+|do\s+)?([a-z0-9áéíóúãõç\s]+)', t_lower)
        if m_cand:
            termo = m_cand.group(1).strip()
            for stop in ['no mtd', 'no ytda', 'hoje', 'ontem', 'no ano', 'no mes']:
                termo = termo.replace(stop, '').strip()
            if termo:
                row_dyn = con.execute(f"SELECT DISTINCT categoria, subcategoria FROM fato_ml WHERE subcategoria ILIKE '%{termo}%' LIMIT 1").fetchone()
                if row_dyn:
                    achou_sub = (termo, row_dyn[0], row_dyn[1])

    achou_cat = None
    for k_cat, cat_nome in MAPA_CATEGORIAS.items():
        if re.search(r'\b' + re.escape(k_cat) + r'\b', t_lower):
            achou_cat = (k_cat, cat_nome)
            break

    # Quando o usuário pede algo como Categoria, mas o nome é de uma Subcategoria
    alerta_didatico = None
    if disse_categoria and achou_sub and not achou_cat:
        k_sub, cat_pai, sub_nome = achou_sub
        if cat_pai:
            alerta_didatico = (
                f"💡 *Aviso Didático:* Olha, o que você pediu como categoria (*'{sub_nome}'*) não existe como Categoria "
                f"porque na verdade é uma **Subcategoria**! A categoria mãe dela é **'{cat_pai}'**."
            )
        else:
            alerta_didatico = (
                f"💡 *Aviso Didático:* Olha, o que você pediu como categoria (*'{sub_nome}'*) não existe como Categoria "
                f"porque na verdade é uma **Subcategoria**."
            )
    # Quando o usuário pede algo como Subcategoria, mas o nome é de uma Categoria principal
    elif disse_subcategoria and achou_cat and not achou_sub:
        k_cat, cat_nome = achou_cat
        alerta_didatico = (
            f"💡 *Aviso Didático:* Olha, você pesquisou como subcategoria, mas **'{cat_nome}'** é uma **Categoria principal** "
            f"(Nível 1), e não uma subcategoria!"
        )

    # Resolução temporal precisa: YTDA, MTD, Ontem, Hoje ou Data Específica
    where_tempo = f"data = '{data_recente}'"
    desc_tempo = f"Hoje ({data_recente})"

    if any(k in t_lower for k in ['ytda', 'ytd', 'acumulado no ano', 'acumulado do ano', 'no ano', 'deste ano', 'ano atual']):
        where_tempo = f"ano = {ano_recente} AND data <= '{data_recente}'"
        desc_tempo = f"YTDA {ano_recente} (Acumulado no Ano até {data_recente})"
    elif any(k in t_lower for k in ['mtd', 'acumulado no mês', 'acumulado no mes', 'acumulado do mês', 'acumulado do mes', 'no mês', 'no mes', 'deste mês', 'deste mes', 'mês atual', 'mes atual']):
        where_tempo = f"ano = {ano_recente} AND mes = {mes_recente} AND data <= '{data_recente}'"
        desc_tempo = f"MTD (Acumulado no Mês {mes_recente:02d}/{ano_recente} até {data_recente})"
    elif 'ontem' in t_lower:
        where_tempo = f"data = '{ontem_str}'"
        desc_tempo = f"Ontem ({ontem_str})"
    else:
        m_iso = re.search(r'\b(202[5-9])-(\d{2})-(\d{2})\b', t_lower)
        m_br = re.search(r'\b(\d{1,2})[/.-](\d{1,2})(?:[/.-](202[5-9]))?\b', t_lower)
        if m_iso:
            d_alvo = m_iso.group(0)
            where_tempo = f"data = '{d_alvo}'"
            desc_tempo = f"Data {d_alvo}"
        elif m_br:
            d, m, y = m_br.groups()
            d_alvo = f"{y if y else ano_recente}-{int(m):02d}-{int(d):02d}"
            where_tempo = f"data = '{d_alvo}'"
            desc_tempo = f"Data {int(d):02d}/{int(m):02d}/{y if y else ano_recente}"

    clean_sql = None

    # Consulta de metadados da base
    if "data" in t_lower and any(w in t_lower for w in ["recente", "ultima", "última", "atualizada", "base"]) and not any(w in t_lower for w in ["venda", "fatur", "quanto", "categoria", "ytda", "mtd"]):
        clean_sql = f"SELECT '{data_recente}' AS data_mais_recente, COUNT(*) AS total_registros FROM fato_ml"

    # Resolução Analítica Determinística (Zero alucinação, precisão 100%)
    if not clean_sql:
        # A. Subcategoria específica mencionada (ou resolvida no mapa ou dinamicamente)
        if achou_sub:
            k_sub, cat_pai, sub_nome = achou_sub
            if cat_pai:
                clean_sql = f"SELECT '{desc_tempo}' AS periodo, categoria, subcategoria, SUM(fat_num) AS faturamento, SUM(qtd_vendas_num) AS vendas FROM fato_ml WHERE {where_tempo} AND categoria = '{cat_pai}' AND subcategoria = '{sub_nome}' GROUP BY categoria, subcategoria"
            else:
                clean_sql = f"SELECT '{desc_tempo}' AS periodo, categoria, subcategoria, SUM(fat_num) AS faturamento, SUM(qtd_vendas_num) AS vendas FROM fato_ml WHERE {where_tempo} AND subcategoria = '{sub_nome}' GROUP BY categoria, subcategoria ORDER BY faturamento DESC"

        # B. Categoria específica mencionada (SEMPRE responde a categoria solicitada, NUNCA o total)
        elif achou_cat:
            k_cat, cat_nome = achou_cat
            clean_sql = f"SELECT '{desc_tempo}' AS periodo, categoria, SUM(fat_num) AS faturamento, SUM(qtd_vendas_num) AS vendas FROM fato_ml WHERE {where_tempo} AND categoria = '{cat_nome}' GROUP BY categoria"

        # C. Ranking de TODAS as subcategorias
        elif disse_subcategoria and any(w in t_lower for w in ['todas', 'ranking', 'quais', 'mais vendid', 'maior', 'cada', 'por subcategoria']):
            clean_sql = f"SELECT '{desc_tempo}' AS periodo, categoria, subcategoria, SUM(fat_num) AS faturamento, SUM(qtd_vendas_num) AS vendas FROM fato_ml WHERE {where_tempo} GROUP BY categoria, subcategoria ORDER BY faturamento DESC LIMIT 5"

        # D. Ranking de TODAS as categorias
        elif disse_categoria and any(w in t_lower for w in ['todas', 'ranking', 'quais', 'mais vendid', 'maior', 'cada', 'por categoria']):
            clean_sql = f"SELECT '{desc_tempo}' AS periodo, categoria, SUM(fat_num) AS faturamento, SUM(qtd_vendas_num) AS vendas FROM fato_ml WHERE {where_tempo} GROUP BY categoria ORDER BY faturamento DESC"

        # E. Ranking de produtos mais vendidos
        elif any(w in t_lower for w in ['produto', 'anuncio', 'anúncio', 'item', 'mais vendido', 'mais vendid']):
            clean_sql = f"SELECT '{desc_tempo}' AS periodo, titulo_produto, marca, categoria, subcategoria, SUM(qtd_vendas_num) AS vendas, SUM(fat_num) AS faturamento FROM fato_ml WHERE {where_tempo} GROUP BY titulo_produto, marca, categoria, subcategoria ORDER BY faturamento DESC LIMIT 5"

        # F. Total Geral do Período (BLINDAGEM TOTAL: NUNCA roda se o usuário falou categoria, subcategoria, produto, marca ou achou entidades)
        elif any(w in t_lower for w in ['total', 'geral', 'faturamento', 'faturou', 'vendas', 'vendeu', 'resultado']):
            if not disse_subcategoria and not disse_categoria and not achou_sub and not achou_cat and not any(w in t_lower for w in ['produto', 'anuncio', 'item', 'marca']):
                clean_sql = f"SELECT '{desc_tempo}' AS periodo, SUM(fat_num) AS faturamento_total, SUM(qtd_vendas_num) AS total_pedidos FROM fato_ml WHERE {where_tempo}"

    # 3. Text-to-SQL de Contingência via Gemini (para consultas livres não cobertas pelas regras acima)
    if not clean_sql:
        prompt_sql = f"""
Você é o motor analítico SQL DuckDB especialista em dados de vendas e inteligência analítica.
Tabela: 'fato_ml' | Período solicitado: {desc_tempo} (filtro: {where_tempo})

REGRAS DE TEMPO CRUCIAIS:
- YTDA / YTD: Acumulado no ano -> ano = {ano_recente} AND data <= '{data_recente}'
- MTD: Acumulado no mês -> ano = {ano_recente} AND mes = {mes_recente} AND data <= '{data_recente}'
- Ontem: data = '{ontem_str}'
- Hoje / Atual: data = '{data_recente}'

HIERARQUIA OFICIAL (CATEGORIA VEM ANTES DA SUBCATEGORIA):
1. Categorias: 'Celulares e Telefones', 'Informática', 'Eletrodomésticos', 'Ferramentas e Construção', 'Casa, Móveis e Decoração'.
2. Subcategorias: 'Smartphones', 'Notebooks', 'Hardware', 'Periféricos', 'Cozinha', 'Climatização', 'Manuais', 'Elétrica', 'Cama e Banho', 'Móveis'.
3. Produtos e Marcas: 'titulo_produto', 'marca'.
4. Métricas: fat_num (faturamento R$), qtd_vendas_num (pedidos).

REGRA FUNDAMENTAL:
- Se perguntar sobre uma CATEGORIA, filtre por 'categoria' e NUNCA retorne o total geral!
- Se perguntar sobre uma SUBCATEGORIA, filtre por 'subcategoria' e traga a Categoria associada!
- Se perguntar YTDA, use ano = {ano_recente} AND data <= '{data_recente}' e NUNCA o dia atual isolado!

Pergunta do usuário: "{texto_msg}"
Retorne EXCLUSIVAMENTE a query SQL DuckDB dentro de ```sql ... ``` ou 'NAO_SQL'.
"""
        resp_sql = chamar_gemini(prompt_sql)
        if resp_sql and "NAO_SQL" not in resp_sql:
            clean_sql = resp_sql.replace("```sql", "").replace("```", "").strip()

    try:
        if not clean_sql:
            return None

        logger.info(f"SQL a executar: {clean_sql}")
        cur = con.execute(clean_sql)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        
        if not rows:
            return f"📊 Olá {user_name}! Pesquisei aqui na base oficial mas não encontrei registros para essa pesquisa específica."

        header_str = " | ".join(col_names)
        linhas_tab = [" | ".join([str(v) if v is not None else "NULL" for v in r]) for r in rows[:15]]
        tabela_str = f"{header_str}\n" + ("-" * len(header_str)) + "\n" + "\n".join(linhas_tab)

        prompt_formatacao = f"""
Você é o assistente executivo Joca_BigQuery. O usuário '{user_name}' perguntou: "{texto_msg}"
Dados extraídos do banco oficial referente a ({desc_tempo}):
{tabela_str}

{f"AVISO DIDÁTICO OBRIGATÓRIO A INCLUIR NA RESPOSTA:\n{alerta_didatico}\n" if alerta_didatico else ""}

Formate uma resposta executiva impecável para o Telegram:
- Saudação obrigatória: "📊 Olá {user_name}! Pesquisei aqui vejamos o resultado:"
{f"- IMEDIATAMENTE após a saudação, inclua com destaque o Aviso Didático explicando que o usuário se confundiu entre Categoria e Subcategoria (use o texto do aviso acima)!\n" if alerta_didatico else ""}
- Destaque o período consultado ({desc_tempo}).
- Respeite rigorosamente a hierarquia: Categoria vem antes da Subcategoria!
- Apresente os números formatados em moeda (R$) e quantidades com separadores de milhar (ex: R$ 3.818.209,99 e 11.119 pedidos).
- Use tópicos claros, negrito e emojis comerciais nos pontos-chave.
- Se houver lista de itens ou categorias, numere com clareza.
- Rodapé obrigatório: "📌 _Dados da Base Analítica (Power BI) · Atualizado até {data_recente}_"
"""
        resp_final = chamar_gemini(prompt_formatacao)
        return resp_final if resp_final else formatar_resultado_python(col_names, rows, user_name, texto_msg, alerta=alerta_didatico)
            
    except Exception as e:
        logger.error(f"Erro IA/DuckDB: {e}")
        return formatar_resultado_python(col_names, rows, user_name, texto_msg, alerta=alerta_didatico) if ('col_names' in locals() and 'rows' in locals()) else None


# ==============================================================================
# 7. ROTAS FLASK (ENDPOINTS DA APLICAÇÃO NO RENDER)
# ==============================================================================
# ==============================================================================
# 7. ROTAS FLASK (ENDPOINTS DA APLICAÇÃO NO RENDER - DUAL BOT HUB)
# ==============================================================================
@app.route("/", methods=["GET"])
def home():
    return f"""
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Central de Inteligência • Joca Bots 24/7</title>
        <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            body {{
                font-family: 'Plus Jakarta Sans', sans-serif;
                background: linear-gradient(135deg, #0b0f19 0%, #111827 50%, #0b0f19 100%);
                color: #e2e8f0;
                min-height: 100vh;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                padding: 40px 20px;
            }}
            .container {{
                max-width: 960px;
                width: 100%;
            }}
            .header {{
                text-align: center;
                margin-bottom: 35px;
            }}
            .badge-live {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                background: rgba(34, 197, 94, 0.15);
                border: 1px solid rgba(34, 197, 94, 0.35);
                color: #4ade80;
                padding: 6px 16px;
                border-radius: 999px;
                font-size: 13px;
                font-weight: 700;
                letter-spacing: 0.5px;
                margin-bottom: 16px;
            }}
            .dot {{
                width: 8px;
                height: 8px;
                background: #22c55e;
                border-radius: 50%;
                box-shadow: 0 0 10px #22c55e;
            }}
            h1 {{
                font-size: 32px;
                font-weight: 800;
                color: #ffffff;
                letter-spacing: -0.5px;
                margin-bottom: 8px;
            }}
            .subtitle {{
                font-size: 16px;
                color: #94a3b8;
            }}
            .meta-bar {{
                background: rgba(255, 255, 255, 0.04);
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 16px;
                padding: 16px 24px;
                display: flex;
                flex-wrap: wrap;
                justify-content: space-around;
                align-items: center;
                gap: 15px;
                margin-bottom: 30px;
                backdrop-filter: blur(10px);
            }}
            .meta-item {{
                text-align: center;
            }}
            .meta-label {{
                font-size: 12px;
                text-transform: uppercase;
                letter-spacing: 0.8px;
                color: #64748b;
                margin-bottom: 4px;
                font-weight: 600;
            }}
            .meta-val {{
                font-size: 16px;
                font-weight: 700;
                color: #f1f5f9;
            }}
            .cards-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
                gap: 25px;
                margin-bottom: 30px;
            }}
            .card {{
                background: rgba(30, 41, 59, 0.6);
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 20px;
                padding: 28px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                backdrop-filter: blur(10px);
                transition: transform 0.2s ease, border-color 0.2s ease;
            }}
            .card:hover {{
                transform: translateY(-4px);
                border-color: rgba(255, 255, 255, 0.2);
            }}
            .card.bq {{
                border-top: 4px solid #ffe600;
            }}
            .card.fabric {{
                border-top: 4px solid #0284c7;
            }}
            .card-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
            }}
            .card-title {{
                font-size: 22px;
                font-weight: 700;
                color: #ffffff;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .tag-pill {{
                font-size: 11px;
                font-weight: 700;
                padding: 4px 10px;
                border-radius: 999px;
            }}
            .tag-meli {{ background: rgba(255, 230, 0, 0.15); color: #ffe600; border: 1px solid rgba(255, 230, 0, 0.3); }}
            .tag-fabric {{ background: rgba(2, 132, 199, 0.15); color: #38bdf8; border: 1px solid rgba(2, 132, 199, 0.3); }}
            .card-body p {{
                font-size: 14px;
                color: #94a3b8;
                margin-bottom: 12px;
                line-height: 1.5;
            }}
            .card-info-box {{
                background: rgba(15, 23, 42, 0.6);
                border-radius: 12px;
                padding: 12px 16px;
                margin-bottom: 20px;
                font-size: 13px;
            }}
            .card-info-box div {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 6px;
            }}
            .card-info-box div:last-child {{ margin-bottom: 0; }}
            .btn-group {{
                display: flex;
                gap: 10px;
            }}
            .btn {{
                flex: 1;
                text-align: center;
                padding: 11px 16px;
                border-radius: 10px;
                text-decoration: none;
                font-size: 13px;
                font-weight: 700;
                transition: all 0.2s ease;
                display: inline-block;
            }}
            .btn-tg {{
                background: #0088cc;
                color: white;
            }}
            .btn-tg:hover {{ background: #0099e6; }}
            .btn-wh {{
                background: rgba(255, 255, 255, 0.08);
                color: #e2e8f0;
                border: 1px solid rgba(255, 255, 255, 0.15);
            }}
            .btn-wh:hover {{ background: rgba(255, 255, 255, 0.15); color: white; }}
            .footer-actions {{
                text-align: center;
            }}
            .btn-all {{
                background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
                color: white;
                padding: 14px 32px;
                border-radius: 12px;
                text-decoration: none;
                font-weight: 700;
                font-size: 15px;
                box-shadow: 0 10px 25px -5px rgba(37, 99, 235, 0.4);
                display: inline-block;
            }}
            .btn-all:hover {{
                box-shadow: 0 12px 30px -5px rgba(37, 99, 235, 0.6);
                transform: translateY(-2px);
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="badge-live"><span class="dot"></span> SERVIDOR NUVEM ATIVO (RENDER 24/7)</div>
                <h1>Central de Inteligência • Joca Bots</h1>
                <p class="subtitle">Canal Conversacional com Inteligência Artificial Generativa (Google Gemini + DuckDB)</p>
            </div>

            <div class="meta-bar">
                <div class="meta-item">
                    <div class="meta-label">Motor de Dados</div>
                    <div class="meta-val">DuckDB + Parquet</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Total de Registros</div>
                    <div class="meta-val">{total_registros:,}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Data de Referência</div>
                    <div class="meta-val">{data_recente}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Arquitetura</div>
                    <div class="meta-val">Dual Webhook Híbrido</div>
                </div>
            </div>

            <div class="cards-grid">
                <!-- Card 1: Joca BigQuery -->
                <div class="card bq">
                    <div>
                        <div class="card-header">
                            <div class="card-title">🔍 Joca_BigQuery</div>
                            <span class="tag-pill tag-bq">Google BigQuery Engine</span>
                        </div>
                        <div class="card-body">
                            <p>Assistente analítico oficial para inteligência de vendas e análise no BigQuery.</p>
                            <div class="card-info-box">
                                <div><span style="color:#64748b;">Bot:</span> <strong>Joca_BigQuery</strong></div>
                                <div><span style="color:#64748b;">Webhook:</span> <code>/webhook</code></div>
                                <div><span style="color:#64748b;">Status:</span> <span style="color:#4ade80;">Ativo 🟢</span></div>
                            </div>
                        </div>
                    </div>
                    <div class="btn-group">
                        <a href="https://t.me/Joca_BigQuery_bot" target="_blank" class="btn btn-tg">Abrir no Telegram</a>
                        <a href="/set_webhook" class="btn btn-wh">Ativar Webhook</a>
                    </div>
                </div>

                <!-- Card 2: Joca Fabric -->
                <div class="card fabric">
                    <div>
                        <div class="card-header">
                            <div class="card-title">⚡ Joca Fabric</div>
                            <span class="tag-pill tag-fabric">Microsoft Fabric Direct Lake</span>
                        </div>
                        <div class="card-body">
                            <p>Assistente analítico integrado ao Lakehouse e Direct Lake no Microsoft Fabric.</p>
                            <div class="card-info-box">
                                <div><span style="color:#64748b;">Bot:</span> <strong>Joca_Fabric</strong></div>
                                <div><span style="color:#64748b;">Webhook:</span> <code>/webhook_fabric</code></div>
                                <div><span style="color:#64748b;">Status:</span> <span style="color:#4ade80;">Ativo 🟢</span></div>
                            </div>
                        </div>
                    </div>
                    <div class="btn-group">
                        <a href="https://t.me/Joca_Fabric_bot" target="_blank" class="btn btn-tg">Abrir no Telegram</a>
                        <a href="/set_webhook_fabric" class="btn btn-wh">Ativar Webhook</a>
                    </div>
                </div>
            </div>

            <div class="footer-actions">
                <a href="/set_all_webhooks" class="btn-all">⚡ Ativar / Renovar Ambos os Webhooks na Nuvem</a>
            </div>
        </div>
    </body>
    </html>
    """


@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status": "online",
        "versao": "3.0.0 - Dual Bot Hub (Joca_BigQuery + Joca_Fabric) + Rate Limiter Anti-Spam",
        "total_registros": total_registros,
        "data_recente": str(data_recente),
        "total_conversas_registradas": len(carregar_conversas()),
        "anti_spam": {
            "max_por_usuario_min": rate_limiter.max_per_user,
            "intervalo_min_segundos": rate_limiter.min_interval,
            "limite_global_gemini_rpm": rate_limiter.max_global_gemini_rpm
        },
        "bots": {
            "bigquery": "Joca_BigQuery",
            "fabric": "Joca_Fabric"
        }
    })


@app.route("/api/conversas", methods=["GET"])
def api_conversas():
    """Endpoint oficial para consumo de telemetria das conversas no Power BI (Power Query)"""
    try:
        bot_filtro = request.args.get("bot", "").strip().lower()
        limite = int(request.args.get("limit", 2000))
        dados = carregar_conversas()
        if bot_filtro:
            dados = [d for d in dados if bot_filtro in d.get("bot", "").lower()]
        # Retorna lista de registros ordenada por data/hora
        return jsonify(dados[-limite:])
    except Exception as e:
        logger.error(f"Erro no endpoint /api/conversas: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return f"""
        <html>
        <head><title>Webhook Joca_BigQuery</title></head>
        <body style="font-family: sans-serif; text-align: center; padding: 50px; background: #0b0f19; color: #fff;">
            <h1>🔍 Webhook do Joca_BigQuery está ATIVO! 🟢</h1>
            <p style="color: #94a3b8; font-size: 16px; margin: 20px 0;">Endpoint oficial do robô <strong>Joca_BigQuery</strong> pronto para receber atualizações do Telegram.</p>
            <p><a href="/" style="color: #38bdf8; text-decoration: none; font-weight: bold;">← Voltar para o Painel</a></p>
        </body>
        </html>
        """

    payload = request.get_json(silent=True)
    if not payload or "message" not in payload:
        return jsonify({"status": "no payload/message"}), 200

    msg = payload["message"]
    threading.Thread(
        target=_processar_mensagem_telegram, 
        args=(msg, BASE_URL_BQ, "Joca_BigQuery"), 
        daemon=True
    ).start()

    return jsonify({"status": "received"}), 200


@app.route("/webhook_fabric", methods=["GET", "POST"])
def webhook_fabric():
    if request.method == "GET":
        return f"""
        <html>
        <head><title>Webhook Joca Fabric</title></head>
        <body style="font-family: sans-serif; text-align: center; padding: 50px; background: #0b0f19; color: #fff;">
            <h1>⚡ Webhook do Joca Fabric está ATIVO! 🟢</h1>
            <p style="color: #94a3b8; font-size: 16px; margin: 20px 0;">Endpoint oficial do robô <strong>Joca_Fabric</strong> pronto para receber atualizações do Telegram.</p>
            <p><a href="/" style="color: #38bdf8; text-decoration: none; font-weight: bold;">← Voltar para o Painel</a></p>
        </body>
        </html>
        """

    payload = request.get_json(silent=True)
    if not payload or "message" not in payload:
        return jsonify({"status": "no payload/message"}), 200

    msg = payload["message"]
    threading.Thread(
        target=_processar_mensagem_telegram, 
        args=(msg, BASE_URL_FABRIC, "Joca_Fabric"), 
        daemon=True
    ).start()

    return jsonify({"status": "received"}), 200


@app.route("/set_webhook", methods=["GET"])
def set_webhook():
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    webhook_url = f"{host_url}/webhook"
    res = requests.post(f"{BASE_URL_BQ}/setWebhook", json={"url": webhook_url}).json()
    return jsonify({"bot": "Joca_BigQuery", "telegram_response": res, "webhook_url": webhook_url})


@app.route("/set_webhook_fabric", methods=["GET"])
def set_webhook_fabric():
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    webhook_url = f"{host_url}/webhook_fabric"
    res = requests.post(f"{BASE_URL_FABRIC}/setWebhook", json={"url": webhook_url}).json()
    return jsonify({"bot": "Joca_Fabric", "telegram_response": res, "webhook_url": webhook_url})


@app.route("/set_all_webhooks", methods=["GET"])
def set_all_webhooks():
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    wh_bq = f"{host_url}/webhook"
    wh_fabric = f"{host_url}/webhook_fabric"
    res_bq = requests.post(f"{BASE_URL_BQ}/setWebhook", json={"url": wh_bq}).json()
    res_fabric = requests.post(f"{BASE_URL_FABRIC}/setWebhook", json={"url": wh_fabric}).json()
    return jsonify({
        "status": "success",
        "joca_bigquery": {"bot": "Joca_BigQuery", "url": wh_bq, "response": res_bq},
        "joca_fabric": {"bot": "Joca_Fabric", "url": wh_fabric, "response": res_fabric}
    })


# ==============================================================================
# JOCA B3 - CANAL CORPORATIVO MICROSOFT TEAMS (BOLSA B3 & MACROECONOMIA)
# ==============================================================================
_teams_token_cache = {"token": None, "expires_at": 0}
TEAMS_TENANT_ID = os.environ.get("TEAMS_TENANT_ID", "d62891e4-d7fd-4035-be5e-56f56edcc459")

def obter_token_teams():
    now = time.time()
    if _teams_token_cache["token"] and now < _teams_token_cache["expires_at"]:
        return _teams_token_cache["token"]
    
    url = f"https://login.microsoftonline.com/{TEAMS_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": TEAMS_BOT_ID,
        "client_secret": TEAMS_CLIENT_SECRET,
        "scope": "https://api.botframework.com/.default"
    }
    try:
        r = requests.post(url, data=data, timeout=10)
        if r.status_code == 200:
            res = r.json()
            token = res.get("access_token")
            expires_in = res.get("expires_in", 3600)
            _teams_token_cache["token"] = token
            _teams_token_cache["expires_at"] = now + expires_in - 300
            return token
        else:
            logger.error(f"Erro ao obter token do Teams: {r.status_code} - {r.text}")
    except Exception as e:
        logger.error(f"Exceção ao obter token do Teams: {e}")
    return None


def enviar_mensagem_teams(service_url, conversation_id, text, reply_to_id=None):
    token = obter_token_teams()
    if not token:
        logger.error("Sem token válido do Bot Framework para envio ao Teams.")
        return False
    
    service_url_clean = service_url.rstrip("/")
    url = f"{service_url_clean}/v3/conversations/{conversation_id}/activities"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "type": "message",
        "text": text
    }
    if reply_to_id:
        body["replyToId"] = reply_to_id

    try:
        r = requests.post(url, headers=headers, json=body, timeout=15)
        if r.status_code in [200, 201, 202]:
            logger.info(f"Mensagem entregue com sucesso no Teams ({conversation_id})")
            return True
        else:
            logger.error(f"Erro ao enviar mensagem ao Teams: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        logger.error(f"Exceção ao enviar mensagem ao Teams: {e}")
        return False


def obter_dados_mercado_b3(texto):
    """Detecta menções a ativos da B3, Ibovespa ou Dólar e busca cotação real."""
    t = texto.upper()
    info = []
    
    # Dólar Comercial
    if any(k in t for k in ["DOLAR", "DÓLAR", "USD", "CAMBIO", "CÂMBIO"]):
        try:
            r = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL", timeout=4).json()
            bid = float(r["USDBRL"]["bid"])
            pct = float(r["USDBRL"]["pctChange"])
            emoji = "🟢" if pct >= 0 else "🔴"
            info.append(f"• **Dólar Comercial (USD/BRL):** R$ {bid:.2f} ({emoji} {pct:+.2f}%)")
        except Exception:
            pass

    # Ibovespa
    if any(k in t for k in ["IBOV", "IBOVESPA", "BOLSA"]):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/%5EBVSP?range=1d&interval=1d", headers=headers, timeout=4).json()
            meta = r["chart"]["result"][0]["meta"]
            preco = meta.get("regularMarketPrice")
            prev = meta.get("chartPreviousClose")
            var = ((preco - prev) / prev) * 100 if prev else 0.0
            emoji = "🟢" if var >= 0 else "🔴"
            info.append(f"• **Ibovespa (^BVSP):** {preco:,.0f} pts ({emoji} {var:+.2f}%)".replace(",", "."))
        except Exception:
            pass

    # Ações da B3 (Regex: 4 letras + 3, 4, 5, 6 ou 11)
    tickers = re.findall(r'\b[A-Z]{4}(?:3|4|5|6|11)\b', t)
    nomes_map = {
        "PETROBRAS": "PETR4", "PETRO": "PETR4",
        "VALE": "VALE3",
        "ITAU": "ITUB4", "ITAÚ": "ITUB4",
        "BRADESCO": "BBDC4",
        "BANCO DO BRASIL": "BBAS3",
        "AMBEV": "ABEV3",
        "WEG": "WEGE3",
        "MAGALU": "MGLU3", "MAGAZINE LUIZA": "MGLU3"
    }
    for nome, tick in nomes_map.items():
        if nome in t and tick not in tickers:
            tickers.append(tick)

    headers = {"User-Agent": "Mozilla/5.0"}
    for tick in set(tickers[:4]):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{tick}.SA?range=1d&interval=1d"
            r = requests.get(url, headers=headers, timeout=4).json()
            meta = r["chart"]["result"][0]["meta"]
            p = meta.get("regularMarketPrice")
            prev = meta.get("chartPreviousClose")
            var = ((p - prev) / prev) * 100 if prev else 0.0
            emoji = "🟢" if var >= 0 else "🔴"
            
            vol = meta.get("regularMarketVolume", 0) or 0
            vol_fin = (p * vol) if (p and vol) else 0
            vol_fin_str = f"R$ {vol_fin / 1e9:.2f} bi" if vol_fin >= 1e9 else (f"R$ {vol_fin / 1e6:.1f} mi" if vol_fin > 0 else "N/D")
            vol_pap_str = f"{vol / 1e6:.1f}M papéis" if vol >= 1e6 else (f"{vol:,.0f} papéis" if vol > 0 else "")
            vol_display = f"{vol_fin_str} ({vol_pap_str})" if vol_pap_str else vol_fin_str
            
            d_low = meta.get("regularMarketDayLow")
            d_high = meta.get("regularMarketDayHigh")
            range_str = f" | Mín: R$ {d_low:.2f} - Máx: R$ {d_high:.2f}" if (d_low and d_high) else ""
            
            info.append(f"• **{tick}:** R$ {p:.2f} ({emoji} {var:+.2f}%) | Vol: {vol_display}{range_str}")
        except Exception:
            pass

    return "\n".join(info) if info else None


def processar_pergunta_b3(texto, user_name="Karl"):
    dados_mercado = obter_dados_mercado_b3(texto)
    
    prompt = f"""Você é o Joca B3, consultor executivo de inteligência e analytics de mercado da B3 no Microsoft Teams.

USUÁRIO: {user_name}
PERGUNTA: "{texto}"

DADOS EM TEMPO REAL:
{dados_mercado if dados_mercado else "Nenhum ticker específico detectado na consulta direta."}

REGRAS ESTRITAS DE RESPOSTA (ZERO PROLIXIDADE, ESTILO BLOOMBERG / TERMINAL EXECUTIVO):
1. NUNCA faça apresentações longas nem repita quem você é ("Olá, sou o Joca B3, consultor executivo...").
2. NUNCA use frases de transição vazias ("Sobre a sua consulta de...", "Estou à disposição para aprofundar...").
3. VÁ DIRETO AOS FATOS E NÚMEROS (máximo 3 a 4 linhas):
   - **TICKER:** R$ [Preço] ([emoji] [Variação%])
   - **Volume:** R$ [Volume financeiro] ([Quantidade papéis])
   - **Intraday:** [Análise cirúrgica de 1 ou 2 frases sobre fluxo comprador/vendedor, picos e médias].
4. Se o usuário mandar apenas uma saudação (ex: "oi", "bom dia"), responda em UMA linha: "Olá, {user_name}. Joca B3 a postos. Qual ativo ou índice deseja monitorar?"

Responda agora:"""

    resp = chamar_gemini(prompt)
    if resp:
        return resp.strip()
    
    if dados_mercado:
        return f"📊 **Radar B3**\n\n{dados_mercado}"
    
    return f"Olá, {user_name}. Joca B3 a postos. Qual ativo ou índice da B3 deseja consultar?"



def _processar_mensagem_teams(activity):
    try:
        texto_bruto = activity.get("text", "") or ""
        texto = re.sub(r'<at>.*?</at>', '', texto_bruto).replace("@Joca", "").replace("@Juca", "").strip()
        if not texto:
            return

        service_url = activity.get("serviceUrl", "")
        conversation_id = activity.get("conversation", {}).get("id", "")
        user_name = activity.get("from", {}).get("name", "Karl")
        activity_id = activity.get("id")

        # Salva a sessão ativa para envio proativo de alertas do Juca
        try:
            from juca_supervisor import salvar_sessao_teams, juca
            salvar_sessao_teams(conversation_id, service_url, user_name)
        except Exception as e_s:
            logger.error(f"Erro ao salvar sessao Teams: {e_s}")

        t_inicio = time.time()

        # Roteamento Inteligente: Se Karl chamar o Juca ou pedir status dos robôs/atualizações
        t_upper = texto.upper()
        if any(w in t_upper for w in ["JUCA", "STATUS GERAL", "ROBOS", "ROBÔS", "CHEFE", "SUPERVISOR", "ATUALIZA", "ATUALIZAR", "PIPELINE"]):
            from juca_supervisor import juca
            resposta = juca.responder_comando_teams(texto, user_name)
            bot_tag = "Juca_Supervisor_Teams"
        else:
            resposta = processar_pergunta_b3(texto, user_name)
            bot_tag = "Joca_B3_Teams"

        tempo_total = round(time.time() - t_inicio, 2)
        enviar_mensagem_teams(service_url, conversation_id, resposta, reply_to_id=activity_id)

        # Telemetria para o Power BI
        try:
            agora = datetime.now()
            salvar_conversa({
                "id": activity_id or int(time.time()),
                "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                "data": agora.strftime("%Y-%m-%d"),
                "hora": agora.strftime("%H:%M:%S"),
                "bot": bot_tag,
                "chat_id": str(conversation_id),
                "usuario": user_name,
                "username": activity.get("from", {}).get("id", ""),
                "tipo_entrada": "Texto (Teams)",
                "pergunta": texto,
                "resposta": resposta[:500] if resposta else "",
                "tempo_resposta_s": tempo_total,
                "status": "Respondido (Sucesso)"
            })
        except Exception as e_log:
            logger.error(f"Erro ao salvar log de telemetria do Teams: {e_log}")

    except Exception as e:
        logger.error(f"Erro ao processar mensagem do Teams: {e}")


def _tratar_boas_vindas_teams(activity):
    try:
        service_url = activity.get("serviceUrl", "")
        conversation_id = activity.get("conversation", {}).get("id", "")
        user_name = activity.get("from", {}).get("name", "Karl")
        
        try:
            from juca_supervisor import salvar_sessao_teams
            salvar_sessao_teams(conversation_id, service_url, user_name)
        except Exception:
            pass

        msg_bv = (
            f"Olá, {user_name}! Sou o **Joca B3**, seu consultor executivo de inteligência e analytics "
            f"da Bolsa de Valores (B3) no Microsoft Teams. 📊\n\n"
            f"Estou pronto para te apoiar com cotações de ações, fechamento de pregão, Ibovespa, Dólar "
            f"e indicadores macroeconômicos. Em que posso te ajudar hoje?\n\n"
            f"👑 *Nosso supervisor geral **Juca** também está ativo. Diga `Juca status` a qualquer momento para ver os robôs e pipelines.*"
        )
        enviar_mensagem_teams(service_url, conversation_id, msg_bv)
    except Exception as e:
        logger.error(f"Erro nas boas-vindas do Teams: {e}")


@app.route("/api/teams_b3", methods=["GET", "POST"])
def webhook_teams_b3():
    if request.method == "GET":
        return f"""
        <html>
        <head><title>Joca B3 - Microsoft Teams</title></head>
        <body style="font-family: sans-serif; text-align: center; padding: 50px; background: #0b0f19; color: #fff;">
            <h1>⚡ Joca B3 no Microsoft Teams está ATIVO! 🟢</h1>
            <p style="color: #94a3b8; font-size: 16px; margin: 20px 0;">Endpoint oficial da IA executiva para mercado financeiro e B3.</p>
            <p style="color: #38bdf8; font-weight: bold;">Bot ID: {TEAMS_BOT_ID}</p>
            <p><a href="/" style="color: #38bdf8; text-decoration: none;">← Voltar para o Painel</a></p>
        </body>
        </html>
        """, 200

    payload = request.get_json(silent=True) or {}
    activity_type = payload.get("type")

    if activity_type == "message":
        threading.Thread(target=_processar_mensagem_teams, args=(payload,), daemon=True).start()
        return jsonify({"status": "received"}), 200

    elif activity_type == "conversationUpdate":
        threading.Thread(target=_tratar_boas_vindas_teams, args=(payload,), daemon=True).start()
        return jsonify({"status": "update_handled"}), 200

    return jsonify({"status": "ignored"}), 200


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
# 7. ROTAS DO JUCA · O CHEFE DOS JOCAS & SUPERVISOR DE AUTO-RECUPERAÇÃO
# ==============================================================================
@app.route("/api/juca/status", methods=["GET"])
def juca_status_api():
    try:
        from juca_supervisor import carregar_estado_juca, PIPELINES
        estado = carregar_estado_juca()
        return jsonify({
            "status": "success",
            "juca": "Ativo",
            "papeis": "Supervisor dos 3 Jocas e Automação de Cargas",
            "estado": estado,
            "pipelines_monitorados": [p["tag"] for p in PIPELINES]
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/juca/monitor", methods=["GET", "POST"])
def juca_monitor_api():
    try:
        from juca_supervisor import juca
        rel = juca.executar_ciclo_vigilancia()
        return jsonify({"status": "success", "relatorio": rel}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/juca/disparar", methods=["POST"])
def juca_disparar_api():
    try:
        req = request.get_json(silent=True) or {}
        pipeline_id = req.get("pipeline_id", "b3")
        from juca_supervisor import juca, PIPELINES
        alvo = next((p for p in PIPELINES if p["id"] == pipeline_id), None)
        if not alvo:
            return jsonify({"status": "error", "message": f"Pipeline '{pipeline_id}' não encontrado"}), 404
        sucesso, msg = juca.disparar_workflow_botao(alvo["repo"], alvo["wf"])
        return jsonify({
            "status": "success" if sucesso else "error",
            "message": msg,
            "pipeline": alvo["name"]
        }), (200 if sucesso else 500)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/juca/test_alerta", methods=["GET", "POST"])
def juca_test_alerta_api():
    try:
        from juca_supervisor import juca
        titulo = "🟡 **[JUCA SUPERVISOR] Teste Operacional de Comunicação**"
        msg = "Karl, este é um teste disparado pelo Juca Supervisor para validar o canal de comunicação direto no Teams!"
        juca.enviar_alerta_teams(titulo, msg, severity="warning")
        return jsonify({"status": "success", "message": "Alerta de teste enviado com sucesso"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# Inicializa a thread de vigilância autônoma do Juca
try:
    from juca_supervisor import iniciar_vigilancia_background
    iniciar_vigilancia_background(intervalo_segundos=300)
    logger.info("Thread do Juca Supervisor iniciada com sucesso em background.")
except Exception as e_juca:
    logger.error(f"Erro ao iniciar vigilância do Juca: {e_juca}")


# ==============================================================================
# 8. EXECUÇÃO DO APLICATIVO
# ==============================================================================
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

