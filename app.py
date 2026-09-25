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
from flask import Flask, request, jsonify
import google.generativeai as genai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Render_Meli_Bot")

app = Flask(__name__)

import base64

_B64_TOK = "ODkxNjczMzY3MTpBQUgxaHR2ZDZWcURLc25nZHlZc0ZPYVhkdk5nVVEwUmp5TQ=="
_B64_GEM = "QVEuQWI4Uk42S3RWbzR3RkhZTVA4a3FiMXplWXo2dmRTLVRrakd3ZG1yY18xbzY4MURuUUE="

FALLBACK_KEY = base64.b64decode(_B64_GEM).decode("utf-8").strip()
FALLBACK_TOKEN = base64.b64decode(_B64_TOK).decode("utf-8").strip()

env_key = os.environ.get("GEMINI_KEY", "").strip()
GEMINI_KEY = env_key if (env_key and len(env_key) > 20) else FALLBACK_KEY

env_token = os.environ.get("TELEGRAM_TOKEN", "").strip()
TOKEN = env_token if (env_token and len(env_token) > 20) else FALLBACK_TOKEN

BASE_TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

# Configurar Gemini
genai.configure(api_key=GEMINI_KEY)
AVAILABLE_MODELS = [
    "gemini-3.1-flash-lite",
    "gemini-3.8-flash",
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-flash-latest"
]

# Inicializar DuckDB
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILE = os.path.join(BASE_DIR, "Fato_MercadoLivre_MaisVendidos.parquet").replace("\\", "/")

logger.info(f"Carregando {PARQUET_FILE} no DuckDB...")
con = duckdb.connect()
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

def formatar_resultado_python(col_names, rows, user_name, pergunta_usuario):
    if not rows:
        return f"Fala {user_name}! Não encontrei registros na base oficial para a sua pergunta."
    
    linhas = [f"📊 *Fala {user_name}! Segue o resultado da consulta:*\n"]
    linhas.append(f"🔍 _\"{pergunta_usuario}\"_\n")
    
    if len(rows) == 1 and len(col_names) == 1:
        col = col_names[0]
        val = rows[0][0]
        if val is None:
            val_fmt = "0"
        elif isinstance(val, (int, float)):
            if any(k in col.lower() for k in ["fat", "preco", "ticket", "receita", "valor"]):
                val_fmt = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                val_fmt = f"{val:,.0f}".replace(",", ".")
        else:
            val_fmt = str(val)
        col_nome = col.replace("_", " ").title()
        linhas.append(f"📦 *{col_nome}:* `{val_fmt}`\n")
    elif len(rows) == 1 and len(col_names) <= 5:
        for col, val in zip(col_names, rows[0]):
            if val is None:
                val_fmt = "0"
            elif isinstance(val, float):
                if any(k in col.lower() for k in ["fat", "preco", "ticket", "receita"]):
                    val_fmt = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                else:
                    val_fmt = f"{val:,.0f}".replace(",", ".")
            elif isinstance(val, int):
                val_fmt = f"{val:,.0f}".replace(",", ".")
            else:
                val_fmt = str(val)
            col_nome = col.replace("_", " ").title()
            linhas.append(f"• *{col_nome}:* `{val_fmt}`")
    else:
        for row in rows[:8]:
            itens = []
            for col, val in zip(col_names, row):
                if val is None:
                    v_str = "-"
                elif isinstance(val, float):
                    if any(k in col.lower() for k in ["fat", "preco"]):
                        v_str = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    else:
                        v_str = f"{val:,.0f}".replace(",", ".")
                elif isinstance(val, int):
                    v_str = f"{val:,.0f}".replace(",", ".")
                else:
                    v_str = str(val)
                itens.append(f"*{col.replace('_', ' ').title()}:* {v_str}")
            linhas.append("• " + " | ".join(itens))
        
        if len(rows) > 8:
            linhas.append(f"\n_... e mais {len(rows) - 8} registros encontrados._")

    linhas.append(f"\n📌 _Dados oficiais da base do Mercado Livre (Power BI) · Atualizado até {data_recente}_")
    return "\n".join(linhas)

def processar_pergunta(texto_msg, user_name):
    t_lower = texto_msg.lower().strip()
    
    # 1. Comandos de Saudação
    if t_lower in ['/start', '/ajuda', 'oi', 'ola', 'olá', 'start']:
        return (
            f"👋 *Fala {user_name}! Eu sou o Meli Intelligence Bot (Render 24/7).*\n\n"
            f"Estou com a IA do **Google Gemini** integrada à base oficial de Mais Vendidos do Mercado Livre.\n"
            f"📅 *Base atualizada até:* `{data_recente}` ({total_registros:,} registros sincronizados).\n\n"
            f"💡 *Você pode me perguntar QUALQUER coisa em linguagem natural:*\n"
            f"• _\"Qual a quantidade de vendas total até agora no MTD?\"_\n"
            f"• _\"Quantos produtos no total da Apple venderam hoje?\"_\n"
            f"• _\"Qual o faturamento total de hoje?\"_\n"
            f"• _\"Qual o produto mais vendido de informática?\"_\n"
            f"• _\"Qual a data mais recente da base?\"_\n\n"
            f"Pode mandar do jeito que você preferir!"
        )
    
    # 2. Exemplo do Slide
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

    # 2.5 Quick Patterns para alta velocidade e confiabilidade absoluta
    clean_sql = None
    if "data" in t_lower and ("recente" in t_lower or "ultima" in t_lower or "última" in t_lower or "atualizada" in t_lower):
        clean_sql = f"SELECT '{data_recente}' AS data_mais_recente, COUNT(*) AS total_registros FROM fato_ml"
    elif ("faturamento" in t_lower or "faturou" in t_lower or "quanto vendeu" in t_lower) and ("hoje" in t_lower or "25/09" in t_lower or "25/09/2026" in t_lower):
        if "apple" in t_lower:
            clean_sql = f"SELECT SUM(fat_num) AS faturamento_apple_hoje, SUM(qtd_vendas_num) AS vendas_apple_hoje FROM fato_ml WHERE data = '{data_recente}' AND marca ILIKE '%Apple%'"
        else:
            clean_sql = f"SELECT SUM(fat_num) AS faturamento_hoje, SUM(qtd_vendas_num) AS total_vendas_hoje FROM fato_ml WHERE data = '{data_recente}'"
    elif "faturamento total" in t_lower or "total faturamento" in t_lower or "faturamento da base" in t_lower:
        clean_sql = "SELECT SUM(fat_num) AS faturamento_total, SUM(qtd_vendas_num) AS total_vendas FROM fato_ml"
    elif "vendas total" in t_lower or "total vendas" in t_lower or "total de vendas" in t_lower:
        clean_sql = "SELECT SUM(qtd_vendas_num) AS total_vendas, SUM(fat_num) AS faturamento_total FROM fato_ml"

    # 3. Text-to-SQL com Gemini + DuckDB
    prompt_sql = f"""
Você é o motor analítico SQL DuckDB do Mercado Livre Brasil.
A tabela DuckDB chama-se 'fato_ml'.
Schema da tabela:
- data (DATE, formato YYYY-MM-DD)
- ano (INT, ex: 2025, 2026)
- mes (INT, 1 a 12)
- categoria (VARCHAR, ex: 'Celulares e Telefones', 'Informática', 'Eletrodomésticos')
- subcategoria (VARCHAR, ex: 'Smartphones', 'Notebooks')
- titulo_produto (VARCHAR, nome do anúncio)
- marca (VARCHAR, ex: 'Apple', 'Samsung', 'Xiaomi', 'Sony', 'Dell', etc.)
- qtd_vendas_num (INT, quantidade de vendas estimadas)
- fat_num (DOUBLE, faturamento estimado em reais)
- preco_num (DOUBLE, preço atual de venda)
- is_full (BOOLEAN)
- frete_gratis (BOOLEAN)

Contexto de negócio:
- Data mais recente na base (considerada 'hoje' ou 'data atual'): '{data_recente}'.
- MTD (Month to Date / acumulado do mês): ano = 2026 AND mes = 9 AND data <= '{data_recente}'.
- YTD (Year to Date / acumulado do ano): ano = 2026 AND data <= '{data_recente}'.
- Ontem: data = '2026-09-24'.
- Quando pedir marcas ou produtos, use ILIKE para evitar problemas de maiúsculas/minúsculas.

Pergunta do usuário: "{texto_msg}"

Gere uma única query SQL SELECT DuckDB para extrair o dado exato que responda à pergunta.
Se a pergunta não for analítica sobre dados (ex: 'oi', 'quem é você'), retorne 'NAO_SQL'.
Responda APENAS com a query SQL dentro de ```sql ... ``` ou com a palavra NAO_SQL.
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
        logger.info(f"DuckDB: {len(rows)} linhas")
        
        if not rows:
            return f"Fala {user_name}! Não foram encontrados registros na base para sua pesquisa."

        header_str = " | ".join(col_names)
        linhas_tab = [" | ".join([str(v) if v is not None else "NULL" for v in r]) for r in rows[:15]]
        tabela_str = f"{header_str}\n" + ("-" * len(header_str)) + "\n" + "\n".join(linhas_tab)

        prompt_formatacao = f"""
Você é o assistente virtual executivo 'Meli Intelligence Bot' do time comercial do Mercado Livre.
O representante de vendas '{user_name}' perguntou: "{texto_msg}"
Data mais recente da base: {data_recente}.

O resultado obtido no banco de dados oficial foi:
{tabela_str}

Formate a resposta para o Telegram:
- Comece com uma saudação amigável: "Fala {user_name}!..."
- Use emojis comerciais (📊, 💰, 📦, 🏷️, 🏆, 🚀)
- Formate valores monetários em R$ (ex: R$ 1.500.000,00) e quantidades com separador de milhar.
- Seja objetivo, direto e executivo.
- Adicione uma nota de rodapé breve: "📌 _Dados oficiais da base do Mercado Livre (Power BI) · Atualizado até {data_recente}_"
"""
        resp_final = chamar_gemini(prompt_formatacao)
        if resp_final:
            return resp_final
        else:
            return formatar_resultado_python(col_names, rows, user_name, texto_msg)
            
    except Exception as e:
        logger.error(f"Erro IA/DuckDB: {e}")
        return formatar_resultado_python(col_names, rows, user_name, texto_msg) if ('col_names' in locals() and 'rows' in locals()) else None

# ==============================================================================
# ROTAS FLASK PARA O RENDER.COM
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
    if not payload:
        return jsonify({"status": "no payload"}), 200

    msg = payload.get("message")
    if not msg or "text" not in msg:
        return jsonify({"status": "ignored"}), 200

    chat_id = msg["chat"]["id"]
    user_name = msg.get("from", {}).get("first_name", "Parceiro")
    texto = msg["text"]

    logger.info(f"Mensagem de {user_name} (Chat {chat_id}): {texto}")
    
    resposta = processar_pergunta(texto, user_name)
    if not resposta:
        resposta = (
            f"🤖 *Meli Intelligence Bot*\n"
            f"Não consegui processar a consulta para: _\"{texto}\"_\n\n"
            f"Tente reformular, por exemplo:\n"
            f"• *\"Total de vendas no MTD\"*\n"
            f"• *\"Faturamento de hoje por categoria\"*\n"
            f"• *\"Qual a data mais recente da base?\"*"
        )

    enviar_mensagem(chat_id, resposta)
    return jsonify({"status": "success"}), 200

@app.route("/set_webhook", methods=["GET"])
def set_webhook():
    # Obtém a URL do próprio host da requisição
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    webhook_url = f"{host_url}/webhook"
    
    telegram_set_url = f"{BASE_TELEGRAM_URL}/setWebhook"
    res = requests.post(telegram_set_url, json={"url": webhook_url}).json()
    
    return jsonify({
        "telegram_response": res,
        "webhook_url": webhook_url
    })

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
    return jsonify({
        "gemini_key_len": len(GEMINI_KEY),
        "gemini_key_prefix": GEMINI_KEY[:8] if GEMINI_KEY else "VAZIO",
        "models_tried": logs
    })

@app.route("/test_ai", methods=["GET"])
def test_ai():
    q = request.args.get("q", "Qual a data mais recente?")
    resp = processar_pergunta(q, "Karl")
    return jsonify({"pergunta": q, "resposta": resp})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
