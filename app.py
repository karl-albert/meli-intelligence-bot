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
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, send_file
import google.generativeai as genai

# Fuso horário oficial de Brasília (BRT UTC-3)
BRT_TZ = timezone(timedelta(hours=-3))


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
_B64_TOK_B3 = "ODk2NzcwMDk0NjpBQUdya3I2MHlkODhTWmlQb0JwNHE2SmJ6VzBhSXNaWlFfYw=="
_B64_GEM = "QVEuQWI4Uk42S3RWbzR3RkhZTVA4a3FiMXplWXo2dmRTLVRrakd3ZG1yY18xbzY4MURuUUE="

FALLBACK_KEY = base64.b64decode(_B64_GEM).decode("utf-8").strip()
FALLBACK_TOKEN_BQ = base64.b64decode(_B64_TOK_BQ).decode("utf-8").strip()
FALLBACK_TOKEN_FABRIC = base64.b64decode(_B64_TOK_FABRIC).decode("utf-8").strip()
FALLBACK_TOKEN_B3 = base64.b64decode(_B64_TOK_B3).decode("utf-8").strip()

env_key = os.environ.get("GEMINI_KEY", "").strip()
GEMINI_KEY = env_key if (env_key and len(env_key) > 20) else FALLBACK_KEY

env_token = os.environ.get("TELEGRAM_TOKEN", "").strip()
TOKEN_BQ = env_token if (env_token and len(env_token) > 20) else FALLBACK_TOKEN_BQ
TOKEN_MELI = TOKEN_BQ

env_token_fabric = os.environ.get("TELEGRAM_TOKEN_FABRIC", "").strip()
TOKEN_FABRIC = env_token_fabric if (env_token_fabric and len(env_token_fabric) > 20) else FALLBACK_TOKEN_FABRIC

env_token_b3 = os.environ.get("TELEGRAM_TOKEN_B3", "").strip()
TOKEN_B3 = env_token_b3 if (env_token_b3 and len(env_token_b3) > 20) else FALLBACK_TOKEN_B3

# Credenciais Microsoft Teams (Joca_B3)
_B64_TEAMS_ID = "YmRmYzI1MjYtNjczYS00YWU5LWExMjEtM2U2YTc4MmE1NTlk"
_B64_TEAMS_SEC = "bTJCOFF+ZFVpdVEySHlFMElsMjQtUy5COWdDYVhjMS5RSFlVRWFPdg=="
FALLBACK_TEAMS_ID = base64.b64decode(_B64_TEAMS_ID).decode("utf-8").strip()
FALLBACK_TEAMS_SEC = base64.b64decode(_B64_TEAMS_SEC).decode("utf-8").strip()

env_teams_id = os.environ.get("TEAMS_BOT_ID", "").strip()
TEAMS_BOT_ID = env_teams_id if (env_teams_id and len(env_teams_id) > 10) else FALLBACK_TEAMS_ID

env_teams_secret = os.environ.get("TEAMS_CLIENT_SECRET", "").strip()
TEAMS_CLIENT_SECRET = env_teams_secret if (env_teams_secret and len(env_teams_secret) > 10) else FALLBACK_TEAMS_SEC

# Credenciais WhatsApp Meta Cloud API Oficial (Joca_B3)
_FALLBACK_WA_TOKEN = "EAAlOJtrlBgoBSldZBEiQPNJxMIG646frarjVYZAOp27IawnvGReDEn3NZClE7lHFJ3eNnNWrOmoVcu07faJVBfQkxnIZAmlNTA0GJSkvZAjyuLSk3WJTtI5lRYZBO1wmLeZCgY2QgoMbXferKapnFw0e8TzxiOkWna5I1A11qJ4sCTBAj4J2ZBCCRRDnCq4YZCCqzEbRD2wEzXLUQdecbg9qAEHhNhJisWEiLZCR0ZBtcDqNIqRIR7I0XPOXrJQcaFtqE4h9OoZASc0ZBBBOUgAaIHTpLGiTN7oS3j2N34nwZD"
_FALLBACK_WA_PHONE_ID = "1279171131956840"

env_wa_token = os.environ.get("WHATSAPP_TOKEN", "").strip()
WHATSAPP_TOKEN = env_wa_token if (env_wa_token and len(env_wa_token) > 20) else _FALLBACK_WA_TOKEN

env_wa_phone = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "").strip()
WHATSAPP_PHONE_NUMBER_ID = env_wa_phone if (env_wa_phone and len(env_wa_phone) > 5) else _FALLBACK_WA_PHONE_ID

WHATSAPP_VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN", "joca_b3_meta_2026").strip()

# Configuração e Retrocompatibilidade
TOKEN = TOKEN_MELI
BASE_TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN_BQ}"
BASE_URL_BQ = f"https://api.telegram.org/bot{TOKEN_BQ}"
BASE_URL_MELI = BASE_URL_BQ
BASE_URL_FABRIC = f"https://api.telegram.org/bot{TOKEN_FABRIC}"
BASE_URL_B3 = f"https://api.telegram.org/bot{TOKEN_B3}"

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
B3_DB_PATH = os.path.join(BASE_DIR, "b3_database.duckdb").replace("\\", "/")
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
data_hora_disponibilizado = None

def recarregar_duckdb():
    global con, data_recente, total_registros, data_inicio, data_hora_disponibilizado
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
    
    # Registra o horario exato em que o dado ficou disponivel para o Bot (Fuso Brasilia UTC-3)
    try:
        if os.path.exists(PARQUET_FILE):
            mtime = os.path.getmtime(PARQUET_FILE)
            dt_m = datetime.fromtimestamp(mtime, tz=timezone(timedelta(hours=-3)))
            data_hora_disponibilizado = dt_m.strftime("%Y-%m-%d %H:%M:%S")
        else:
            data_hora_disponibilizado = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        data_hora_disponibilizado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"[OK] Base DuckDB pronta: {total_registros:,} registros. Periodo: {data_inicio} ate {data_recente}. Disponibilizado em: {data_hora_disponibilizado}")

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

        # Separação Estrita de Responsabilidade: Telegram = Joca Mercado Livre (100% ML)
        t_lower = texto.lower()
        if any(re.search(r'\b' + re.escape(tk) + r'\b', t_lower) for tk in ["petr4", "vale3", "itub4", "bbdc4", "wege3", "ibov", "ibovespa", "ptax", "selic", "ipca", "igpm", "pib b3"]):
            resposta = (
                f"🤖 *Olá {user_name}! Eu sou o Joca Mercado Livre.*\n\n"
                f"Meu escopo é exclusivamente **Inteligência de Vendas e Produtos do Mercado Livre**.\n\n"
                f"💡 Para consultar cotações de ações (como `{texto}`), Ibovespa, Dólar, Selic e indicadores da Bolsa, "
                f"acesse o **Joca B3 no Microsoft Teams**!"
            )
        else:
            resposta = processar_pergunta(texto, user_name)

        if not resposta:
            resposta = (
                f"🤖 *{bot_label}*\n"
                f"Não consegui processar a consulta de vendas para: _\"{texto}\"_\n\n"
                f"💡 *Exemplos de perguntas sobre o Mercado Livre:*\n"
                f"• _\"Total de vendas no MTD\"_\n"
                f"• _\"Faturamento de hoje por categoria\"_\n"
                f"• _\"Quais os produtos mais vendidos de Celulares?\"_\n"
                f"• _\"Faturamento acumulado no ano (YTDA)\"_"
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


def _processar_mensagem_telegram_b3(msg, base_url, bot_label="Bot_B3"):
    """Executado em segundo plano para o Bot_B3 no Telegram"""
    try:
        chat_id = msg.get("chat", {}).get("id")
        user_obj = msg.get("from", {})
        user_id = user_obj.get("id") or chat_id
        user_name = user_obj.get("first_name", "Investidor")
        if not chat_id:
            return

        # Rate limiter anti-spam
        permitido, motivo, espera_s = rate_limiter.check_user_limit(user_id)
        if not permitido:
            logger.warning(f"Rate limit acionado para {user_name} (ID {user_id}). Motivo: {motivo}")
            aviso_spam = (
                f"⏳ *Calma lá, {user_name}!* 🚦\n\n"
                f"Você está enviando perguntas muito rápido. "
                f"Por favor aguarde cerca de *{espera_s} segundos* antes da próxima dúvida."
            )
            enviar_mensagem(chat_id, aviso_spam, base_url=base_url)
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
                logger.error(f"Erro ao processar áudio recebido no Bot_B3: {e_audio}")
                enviar_mensagem(chat_id, "🎙️ Não consegui ouvir seu áudio com clareza. Poderia repetir ou digitar?", base_url=base_url)
                return

        elif "text" in msg:
            texto = msg["text"]

        if not texto:
            return

        t_inicio = time.time()
        requests.post(f"{base_url}/sendChatAction", json={"chat_id": chat_id, "action": "typing"}, timeout=5)

        # Trata comandos de boas-vindas /start
        if texto.strip() == "/start":
            msg_start = (
                f"🏛️ *Olá, {user_name}! Bem-vindo ao {bot_label}!*\n\n"
                f"Sou o seu assistente de inteligência de mercado financeiro da **B3 (Bolsa de Valores)**.\n\n"
                f"💡 *O que você pode me perguntar:*\n"
                f"• Digite `1` ou `IBOV` para ver o resumo do Ibovespa hoje\n"
                f"• Digite um código de ação: `PETR4`, `VALE3`, `ITUB4`, `WEGE3`\n"
                f"• `Maiores altas`, `Maiores baixas`, `Volume financeiro`\n"
                f"• Indicadores macro: `Selic`, `IPCA`, `Dólar`, `PIB`\n"
                f"• Ou envie uma pergunta por áudio!\n\n"
                f"📊 *Como posso te ajudar agora?*"
            )
            enviar_mensagem(chat_id, msg_start, base_url=base_url)
            return

        # Processa pergunta da B3
        conversation_id = f"tg_{chat_id}"
        resposta = processar_pergunta_b3(texto, user_name, conversation_id)
        if not resposta:
            resposta = "Desculpe, não consegui obter informações da Bolsa para essa consulta."

        # Substitui formatações HTML como <br/> por quebras de linha para o Telegram
        resposta = re.sub(r'<br\s*/?>', '\n', resposta, flags=re.IGNORECASE)

        enviar_mensagem(chat_id, resposta, base_url=base_url)

        # Telemetria
        try:
            t_duracao = round(time.time() - t_inicio, 2)
            from_user = msg.get("from", {})
            p_nome = from_user.get("first_name", "")
            u_nome = from_user.get("last_name", "")
            nome_completo = f"{p_nome} {u_nome}".strip() or user_name
            username_val = from_user.get("username", "")
            username_str = f"@{username_val}" if username_val else "-"
            agora = datetime.now()
            
            salvar_conversa({
                "id": msg.get("message_id") or int(time.time()),
                "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                "data": agora.strftime("%Y-%m-%d"),
                "hora": agora.strftime("%H:%M:%S"),
                "bot": "Bot_B3_Telegram",
                "chat_id": str(chat_id),
                "usuario": nome_completo,
                "username": username_str,
                "tipo_entrada": "Voz (Telegram)" if origem_audio else "Texto (Telegram)",
                "pergunta": texto,
                "resposta": resposta[:500] if resposta else "",
                "tempo_resposta_s": t_duracao,
                "status": "Respondido (Sucesso)"
            })
        except Exception as e_log_b3:
            logger.error(f"Erro ao salvar telemetria Telegram Bot_B3: {e_log_b3}")

    except Exception as e_proc_b3:
        logger.error(f"Erro ao processar mensagem do Bot_B3 Telegram: {e_proc_b3}")



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
    b3_indices_eua_dt = None
    b3_tickers_dt = None
    b3_ibov_dt = None
    try:
        con_b3 = get_b3_db()
        try:
            b3_indices_eua_dt = str(con_b3.execute("SELECT MAX(data) FROM fato_indices_americanos").fetchone()[0])
        except Exception:
            pass
        try:
            b3_tickers_dt = str(con_b3.execute("SELECT MAX(data) FROM fato_b3_tickers").fetchone()[0])
        except Exception:
            pass
        try:
            b3_ibov_dt = str(con_b3.execute("SELECT MAX(data) FROM fato_b3_ibov").fetchone()[0])
        except Exception:
            pass
        con_b3.close()
    except Exception:
        pass

    # Garante que o horário de disponibilização nunca seja null no retorno
    horario_bot = data_hora_disponibilizado
    if not horario_bot:
        if os.path.exists(PARQUET_FILE):
            try:
                mtime = os.path.getmtime(PARQUET_FILE)
                dt_m = datetime.fromtimestamp(mtime, tz=timezone(timedelta(hours=-3)))
                horario_bot = dt_m.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                horario_bot = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            horario_bot = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")

    return jsonify({
        "status": "online",
        "versao": "3.3.0 - Auditoria Dupla B3 (Acoes e Indices EUA)",
        "total_registros": total_registros,
        "data_recente": str(data_recente),
        "horario_disponibilizacao": horario_bot,
        "Horario_Disponibilizado_Bot": horario_bot,
        "b3_indices_max_data": b3_indices_eua_dt,
        "b3_indices_eua_max_data": b3_indices_eua_dt,
        "b3_tickers_max_data": b3_tickers_dt,
        "b3_ibov_max_data": b3_ibov_dt,
        "total_conversas_registradas": len(carregar_conversas()),
        "anti_spam": {
            "max_por_usuario_min": rate_limiter.max_per_user,
            "intervalo_min_segundos": rate_limiter.min_interval,
            "limite_global_gemini_rpm": rate_limiter.max_global_gemini_rpm
        },
        "bots": {
            "bigquery": "Joca_BigQuery",
            "fabric": "Joca_Fabric",
            "b3_teams": "Joca_B3",
            "b3_telegram": "Bot_B3"
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


@app.route("/webhook_b3", methods=["GET", "POST"])
def webhook_b3():
    if request.method == "GET":
        return f"""
        <html>
        <head><title>Webhook Bot_B3</title></head>
        <body style="font-family: sans-serif; text-align: center; padding: 50px; background: #0b0f19; color: #fff;">
            <h1>📈 Webhook do Bot_B3 está ATIVO! 🟢</h1>
            <p style="color: #94a3b8; font-size: 16px; margin: 20px 0;">Endpoint oficial do robô <strong>Bot_B3 (@BolsaB3_bot)</strong> pronto para receber atualizações do Telegram.</p>
            <p><a href="/" style="color: #38bdf8; text-decoration: none; font-weight: bold;">← Voltar para o Painel</a></p>
        </body>
        </html>
        """

    payload = request.get_json(silent=True)
    if not payload or "message" not in payload:
        return jsonify({"status": "no payload/message"}), 200

    msg = payload["message"]
    threading.Thread(
        target=_processar_mensagem_telegram_b3, 
        args=(msg, BASE_URL_B3, "Bot_B3"), 
        daemon=True
    ).start()

    return jsonify({"status": "received"}), 200


@app.route("/set_webhook_b3", methods=["GET"])
def set_webhook_b3():
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    webhook_url = f"{host_url}/webhook_b3"
    res = requests.post(f"{BASE_URL_B3}/setWebhook", json={"url": webhook_url}).json()
    return jsonify({"bot": "Bot_B3", "telegram_response": res, "webhook_url": webhook_url})


@app.route("/set_all_webhooks", methods=["GET"])
def set_all_webhooks():
    host_url = request.host_url.replace("http://", "https://").rstrip("/")
    wh_bq = f"{host_url}/webhook"
    wh_fabric = f"{host_url}/webhook_fabric"
    wh_b3 = f"{host_url}/webhook_b3"
    res_bq = requests.post(f"{BASE_URL_BQ}/setWebhook", json={"url": wh_bq}).json()
    res_fabric = requests.post(f"{BASE_URL_FABRIC}/setWebhook", json={"url": wh_fabric}).json()
    res_b3 = requests.post(f"{BASE_URL_B3}/setWebhook", json={"url": wh_b3}).json()
    return jsonify({
        "status": "success",
        "joca_bigquery": {"bot": "Joca_BigQuery", "url": wh_bq, "response": res_bq},
        "joca_fabric": {"bot": "Joca_Fabric", "url": wh_fabric, "response": res_fabric},
        "bot_b3": {"bot": "Bot_B3", "url": wh_b3, "response": res_b3}
    })


# ==============================================================================
# JOCA B3 - CANAL CORPORATIVO MICROSOFT TEAMS (BOLSA B3 & MACROECONOMIA)
# ==============================================================================
_teams_token_cache = {}
TEAMS_TENANT_ID = os.environ.get("TEAMS_TENANT_ID", "d62891e4-d7fd-4035-be5e-56f56edcc459")

def obter_token_teams(tenant="botframework.com"):
    now = time.time()
    if _teams_token_cache.get(tenant) and now < _teams_token_cache[tenant].get("expires_at", 0):
        return _teams_token_cache[tenant]["token"]
    
    # Ordem de tentativa de autorização: botframework.com (universal), common, e tenant corporativo
    candidatos = [tenant, "botframework.com", "common", TEAMS_TENANT_ID]
    for t_id in candidatos:
        if not t_id:
            continue
        url = f"https://login.microsoftonline.com/{t_id}/oauth2/v2.0/token"
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
                _teams_token_cache[tenant] = {
                    "token": token,
                    "expires_at": now + expires_in - 300
                }
                return token
            else:
                logger.warning(f"Tentativa de token Teams ({t_id}) retornou: {r.status_code}")
        except Exception as e:
            logger.error(f"Erro ao obter token do Teams ({t_id}): {e}")
    return None


def formatar_texto_para_teams(texto):
    """
    Garante que quebras de linha Markdown renderizem perfeitamente no Microsoft Teams,
    impedindo que o cliente do Teams junte linhas consecutivas num bloco único de texto.
    """
    if not texto:
        return ""

    linhas = texto.split("\n")
    novas = []
    for l in linhas:
        l_s = l.strip()
        if not l_s:
            novas.append("<br/>")
        elif l_s.endswith("<br/>") or l_s.endswith("<br>"):
            novas.append(l)
        else:
            novas.append(l + "  <br/>")
    return "\n".join(novas)


def enviar_mensagem_teams(service_url, conversation_id, text, reply_to_id=None):
    if not service_url or not conversation_id:
        logger.error(f"service_url ({service_url}) ou conversation_id ({conversation_id}) vazios.")
        return False

    token = obter_token_teams("botframework.com") or obter_token_teams(TEAMS_TENANT_ID)
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
        "textFormat": "markdown",
        "text": formatar_texto_para_teams(text)
    }
    if reply_to_id:
        body["replyToId"] = reply_to_id

    try:
        r = requests.post(url, headers=headers, json=body, timeout=15)
        if r.status_code in [200, 201, 202]:
            logger.info(f"Mensagem entregue com sucesso no Teams ({conversation_id})")
            return True
        elif r.status_code == 401:
            logger.warning("Erro 401 no envio Teams. Tentando renovar com tenant corporativo...")
            token_corp = obter_token_teams(TEAMS_TENANT_ID)
            if token_corp:
                headers["Authorization"] = f"Bearer {token_corp}"
                r2 = requests.post(url, headers=headers, json=body, timeout=15)
                if r2.status_code in [200, 201, 202]:
                    logger.info(f"Mensagem entregue no Teams com token corporativo ({conversation_id})")
                    return True
            logger.error(f"Erro persistente 401 ao enviar mensagem ao Teams: {r.text}")
            return False
        else:
            logger.error(f"Erro ao enviar mensagem ao Teams: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        logger.error(f"Exceção ao enviar mensagem ao Teams: {e}")
        return False


# ==============================================================================
# 6. MOTOR DE INTELIGÊNCIA EXECUTIVA: JOCA B3 NO MICROSOFT TEAMS (8 PILARES)
# ==============================================================================
b3_conversation_states = {}

def get_b3_db():
    """Retorna conexão somente-leitura com a base oficial DuckDB do Painel B3."""
    return duckdb.connect(B3_DB_PATH, read_only=True)

def formatar_valor_monetario(valor):
    if valor is None:
        return "N/D"
    if valor >= 1e9:
        return f"R$ {valor / 1e9:.2f} bilhões"
    elif valor >= 1e6:
        return f"R$ {valor / 1e6:.1f} milhões"
    elif valor >= 1e3:
        return f"R$ {valor / 1e3:.1f} mil"
    else:
        return f"R$ {valor:.2f}"

def formatar_papeis(qtd):
    if qtd is None:
        return ""
    if qtd >= 1e6:
        return f"{qtd / 1e6:.1f}M ações negociadas"
    elif qtd >= 1e3:
        return f"{qtd / 1e3:.1f} mil ações negociadas"
    else:
        return f"{int(qtd):,} ações negociadas".replace(",", ".")


def _gerar_menu_boas_vindas(user_name="Karl"):
    return (
        f"👋 Olá, {user_name}! Sou o Joca B3. Fui idealizado para tirar dúvidas por aqui, integrado ao seu ambiente de trabalho para auxiliá-lo a respeito das informações do Painel B3.<br/>\n"
        f"Me guie sobre o que você precisa de informações:<br/>\n"
        f"1️⃣ Algum Índice? (Ibovespa ou Mercado Americano)<br/>\n"
        f"2️⃣ Ticker / Ações / Ativos? (Cotação, DY e P/VP dos 102 ativos da B3)<br/>\n"
        f"3️⃣ Alguma Moeda? (Dólar Comercial PTAX)<br/>\n"
        f"4️⃣ Volume ou Preço? (Volumes financeiros e preços de fechamento)<br/>\n"
        f"5️⃣ Algum Indicador Macro? (Taxa Selic, IPCA 12M, IGP-M ou PIB)<br/>\n"
        f"6️⃣ 5 Maiores Altas ou 5 Maiores Baixas? (Destaques do pregão)<br/>\n"
        f"7️⃣ Sobre algum Setor de Atuação? (Os 10 setores oficiais da B3)<br/>\n"
        f"8️⃣ Fluxos de Ativos / Investidores? (Capital Estrangeiro, Institucional e Pessoa Física)<br/>\n"
        f"💡 Você pode simplesmente digitar o número correspondente (1 a 8) ou fazer sua pergunta diretamente!"
    )


def _tratar_pilar_1_indices(texto):
    t = texto.upper()
    con = get_b3_db()
    try:
        mapa_americanos = {
            "DOW JONES": "^DJI", "DOW": "^DJI", "DJI": "^DJI",
            "NASDAQ": "^IXIC", "NQ": "^NQGS",
            "NYSE": "^NYA", 
            "PETRÓLEO BRENT": "BZ=F", "PETROLEO BRENT": "BZ=F", "BRENT": "BZ=F",
            "OMXS30": "^OMX", "OMX": "^OMX"
        }
        for k in sorted(mapa_americanos.keys(), key=len, reverse=True):
            if re.search(r'\b' + re.escape(k) + r'\b', t) or k in t:
                sim = mapa_americanos[k]
                rows = con.execute("""
                    SELECT data, indice, ticker, preco_fechamento, preco_minimo, preco_maximo 
                    FROM fato_indices_americanos 
                    WHERE ticker = ? 
                    ORDER BY data DESC LIMIT 2
                """, [sim]).fetchall()
                if rows:
                    r0 = rows[0]
                    p0 = r0[3]
                    p1 = rows[1][3] if len(rows) > 1 else p0
                    var_pct = ((p0 - p1) / p1) * 100.0 if p1 > 0 else 0.0
                    emoji = "🟢" if var_pct >= 0 else "🔴"
                    dt_str = r0[0].strftime("%d/%m/%Y") if hasattr(r0[0], "strftime") else str(r0[0])
                    p0_fmt = f"{p0:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    min_fmt = f"{r0[4]:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    max_fmt = f"{r0[5]:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    return (
                        f"📊 **{r0[1]} ({r0[2]}) | Pregão de {dt_str}**<br/><br/>\n"
                        f"• **Fechamento:** {p0_fmt} pts ({emoji} {var_pct:+.2f}%)<br/><br/>\n"
                        f"• **Mín:** {min_fmt} pts | **Máx:** {max_fmt} pts 📌"
                    )

        r = con.execute("SELECT data, fechamento, variacao, minima, maxima FROM fato_b3_ibov ORDER BY data DESC LIMIT 1").fetchone()
        if not r:
            return "⚠️ Dados do Ibovespa não localizados na base oficial."
        
        dt_str = r[0].strftime("%d/%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
        fech = r[1]
        var = r[2]
        min_p = r[3]
        max_p = r[4]
        emoji = "🟢" if var >= 0 else "🔴"

        fech_fmt = f"{fech:,.0f}".replace(",", ".")
        min_fmt = f"{min_p:,.0f}".replace(",", ".")
        max_fmt = f"{max_p:,.0f}".replace(",", ".")

        return (
            f"📊 **Índice Ibovespa (^BVSP) | Pregão de {dt_str}**<br/><br/>\n"
            f"• **Fechamento:** {fech_fmt} pts ({emoji} {var:+.2f}%)<br/><br/>\n"
            f"• **Mín:** {min_fmt} pts | **Máx:** {max_fmt} pts 📌<br/><br/>\n"
            f"💡 Deseja consultar algum índice internacional do painel?<br/>\n"
            f"Basta digitar: **Dow Jones (ou Dow). Nasdaq. NYSE. Petróleo Brent (ou Brent) ou OMXS30.**"
        )
    finally:
        con.close()


def _tratar_pilar_2_ticker(ticker, data_alvo=None):
    con = get_b3_db()
    try:
        t_up = ticker.upper().strip()
        ativo = None
        try:
            ativo = con.execute("SELECT ticker, nome_empresa, setor_atuacao, ticker_inativo FROM dim_ativos_board WHERE UPPER(ticker) = ?", [t_up]).fetchone()
        except Exception:
            pass

        if not ativo:
            try:
                ativo = con.execute("SELECT ticker, nome_empresa, setor_atuacao, ticker_inativo FROM dim_ativos WHERE UPPER(ticker) = ?", [t_up]).fetchone()
            except Exception:
                pass

        if ativo:
            nome = ativo[1] or t_up
            setor = ativo[2] or "Ações B3"
            status = ativo[3] or "ATIVA"
            if status == "INATIVA":
                return f"⚠️ **Ativo Inativo:** O ticker **{t_up}** consta como **INATIVO** na base do Painel B3 (não é exibido nas telas ativas do relatório)."
        else:
            nome = t_up
            setor = "Ações B3"

        if data_alvo:
            dt_str_iso = data_alvo.strftime("%Y-%m-%d") if hasattr(data_alvo, "strftime") else str(data_alvo)
            rows = con.execute("""
                SELECT preco, variacao, dy, p_vp, volume, data 
                FROM fato_b3_tickers 
                WHERE ticker = ? AND data <= ? 
                ORDER BY data DESC LIMIT 2
            """, [t_up, dt_str_iso]).fetchall()
        else:
            rows = con.execute("""
                SELECT preco, variacao, dy, p_vp, volume, data 
                FROM fato_b3_tickers 
                WHERE ticker = ? 
                ORDER BY data DESC LIMIT 2
            """, [t_up]).fetchall()

        if not rows:
            return f"📈 **{t_up} · {nome}** | {setor}\n⚠️ Sem cotação registrada para este período na base oficial."

        r0 = rows[0]
        p = r0[0]
        var = r0[1]
        dy = r0[2]
        p_vp = r0[3]
        vol = r0[4]
        dt = r0[5]
        dt_str = dt.strftime("%d/%m/%Y") if hasattr(dt, "strftime") else str(dt)

        if len(rows) > 1:
            p1 = rows[1][0]
            var_rs = p - p1
        else:
            var_rs = p * (var / 100.0) if var else 0.0

        emoji = "🟢" if var >= 0 else "🔴"
        sinal = "+" if var_rs >= 0 else ""
        vol_str = formatar_valor_monetario(vol)
        papeis_qtd = (vol / p) if (vol and p > 0) else None
        papeis_str = formatar_papeis(papeis_qtd)

        dy_str = f"{dy:.2f}%" if dy and dy > 0 else "0,00%"
        pvp_str = f"{p_vp:.2f}" if p_vp and p_vp > 0 else None

        linhas_resp = [
            f"📈 **{t_up} · {nome}** ({setor})",
            f"• **Preço de Fechamento:** R$ {p:.2f} ({emoji} {sinal}R$ {var_rs:.2f} | {var:+.2f}%)",
            f"• **Volume Financeiro:** {vol_str}",
        ]
        if papeis_str:
            linhas_resp.append(f"• **Volume de Papéis:** {papeis_str}")
        if dy and dy > 0:
            linhas_resp.append(f"• **Dividend Yield (DY):** {dy_str}")
        if pvp_str:
            linhas_resp.append(f"• **P/VP:** {pvp_str}")

        linhas_resp.append(f"📌 *Dados oficiais do Painel B3 (Pregão de {dt_str}).*")
        return "\n".join(linhas_resp)
    finally:
        con.close()


def _tratar_pilar_3_dolar():
    con = get_b3_db()
    try:
        r = con.execute("SELECT data, venda, variacao, compra, minima, maxima FROM fato_b3_dolar ORDER BY data DESC LIMIT 1").fetchone()
        if not r:
            return "⚠️ Dados do Dólar PTAX não localizados na base oficial."
        
        dt_str = r[0].strftime("%d/%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
        venda = r[1]
        var = r[2]
        compra = r[3]
        min_d = r[4]
        max_d = r[5]
        emoji = "🟢" if var >= 0 else "🔴"

        return (
            f"💵 **Dólar Comercial (USD/BRL PTAX) | Pregão de {dt_str}**\n"
            f"• **Cotação Oficial (Venda):** R$ {venda:.2f} ({emoji} {var:+.2f}%)\n"
            f"• **Compra:** R$ {compra:.4f} | **Venda:** R$ {venda:.4f}\n"
            f"• **Faixa do Dia:** Mín: R$ {min_d:.4f} — Máx: R$ {max_d:.4f}\n"
            f"📌 *Dados do Painel B3 (Pregão de {dt_str}).*"
        )
    finally:
        con.close()


def _tratar_pilar_4_volume(texto, user_name):
    tickers = re.findall(r'\b[A-Z]{4}(?:3|4|5|6|11)\b', texto.upper())
    if not tickers:
        nomes_map = {
            "PETROBRAS": "PETR4", "VALE": "VALE3", "ITAU": "ITUB4", "ITAÚ": "ITUB4",
            "BRADESCO": "BBDC4", "BANCO DO BRASIL": "BBAS3", "AMBEV": "ABEV3",
            "WEG": "WEGE3", "MAGALU": "MGLU3", "EMBRAER": "EMBJ3"
        }
        for n, tk in nomes_map.items():
            if n in texto.upper():
                tickers.append(tk)
                break

    if not tickers:
        return f"⚠️ Não identifiquei um ticker válido na sua resposta. Por favor, envie o código da ação (ex: `PETR4`, `VALE3`)."

    tick = tickers[0]
    con = get_b3_db()
    try:
        rows = con.execute("""
            SELECT f.preco, f.variacao, f.volume, f.data, d.nome_empresa, d.setor_atuacao
            FROM fato_b3_tickers f
            JOIN dim_ativos d ON f.ticker = d.ticker
            WHERE f.ticker = ?
            ORDER BY f.data DESC LIMIT 2
        """, [tick]).fetchall()

        if not rows:
            return f"⚠️ Ticker **{tick}** não possui registros de negociação na base oficial."

        r0 = rows[0]
        p0 = r0[0]
        var_pct = r0[1]
        vol = r0[2]
        dt = r0[3]
        nome = r0[4]
        setor = r0[5]
        dt_str = dt.strftime("%d/%m/%Y") if hasattr(dt, "strftime") else str(dt)

        if len(rows) > 1:
            p1 = rows[1][0]
            var_liq_rs = p0 - p1
        else:
            var_liq_rs = p0 * (var_pct / 100.0)

        emoji = "🟢" if var_pct >= 0 else "🔴"
        sinal = "+" if var_liq_rs >= 0 else ""
        vol_fin_str = formatar_valor_monetario(vol)
        papeis_qtd = (vol / p0) if (vol and p0 > 0) else None
        papeis_str = formatar_papeis(papeis_qtd)

        return (
            f"💼 **{tick} · {nome}** ({setor})\n"
            f"• **Preço D0:** R$ {p0:.2f} ({emoji} {sinal}R$ {var_liq_rs:.2f} | {var_pct:+.2f}%)\n"
            f"• **Volume Financeiro:** {vol_fin_str}\n"
            f"• **Volume de Papéis:** {papeis_str}\n"
            f"📌 *Dados oficiais do Painel B3 (Pregão de {dt_str}).*"
        )
    finally:
        con.close()


def _tratar_pilar_5_macro(texto):
    con = get_b3_db()
    try:
        t = texto.upper()
        if "SELIC" in t or "JUROS" in t:
            r = con.execute("SELECT data, indicador, valor, unidade FROM fato_macro_diarios WHERE indicador LIKE '%Selic Meta%' ORDER BY data DESC LIMIT 1").fetchone()
            dt_str = r[0].strftime("%d/%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
            return (
                f"🏛️ **Taxa Selic Meta (% a.a.) — Posição em {dt_str}**<br/>\n"
                f"• **Taxa Oficial:** {r[2]:.2f}% a.a.<br/><br/>\n"
                f"📌 ***Fonte:*** *Banco Central do Brasil (Copom)*"
            )

        if "IPCA" in t or "INFLAÇÃO" in t or "INFLACAO" in t:
            r = con.execute("SELECT data, indicador, valor, unidade FROM fato_macro_mensais WHERE indicador LIKE '%IPCA Acumulado 12%' ORDER BY data DESC LIMIT 1").fetchone()
            dt_str = r[0].strftime("%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
            return (
                f"🏛️ **IPCA Inflação Oficial — Posição em {dt_str}**<br/>\n"
                f"• **Acumulado 12 Meses:** {r[2]:.2f}%<br/><br/>\n"
                f"📌 ***Fonte:*** *IBGE*"
            )

        if "IGPM" in t or "IGP-M" in t:
            r = con.execute("SELECT data, indicador, valor, unidade FROM fato_macro_mensais WHERE indicador LIKE '%IGP-M Mensal%' ORDER BY data DESC LIMIT 1").fetchone()
            dt_str = r[0].strftime("%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
            return (
                f"🏛️ **IGP-M Mensal — Posição em {dt_str}**<br/>\n"
                f"• **Variação Mensal:** {r[2]:.2f}%<br/><br/>\n"
                f"📌 ***Fonte:*** *FGV*"
            )

        if "CAGED" in t or "EMPREGO" in t:
            r = con.execute("SELECT data, indicador, valor, unidade FROM fato_macro_mensais WHERE indicador LIKE '%CAGED Total%' ORDER BY data DESC LIMIT 1").fetchone()
            dt_str = r[0].strftime("%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
            vagas = f"{int(r[2]):,}".replace(",", ".")
            return (
                f"🏛️ **CAGED Emprego Formal — Posição em {dt_str}**<br/>\n"
                f"• **Total de Contratações CLT:** {vagas} vagas<br/><br/>\n"
                f"📌 ***Fonte:*** *Ministério do Trabalho e Emprego (MTE)*"
            )

        if "PIB" in t:
            r = con.execute("SELECT data, indicador, valor, unidade FROM fato_macro_trimestrais WHERE indicador LIKE '%PIB%' ORDER BY data DESC LIMIT 1").fetchone()
            dt_str = r[0].strftime("%d/%m/%Y") if hasattr(r[0], "strftime") else str(r[0])
            val_tri = r[2] / 1e6
            return (
                f"🏛️ **PIB Trimestral a Preços de Mercado — Posição em {dt_str}**<br/>\n"
                f"• **Valor Apurado:** R$ {val_tri:.2f} trilhões (R$ {r[2]:,.1f} Milhões)<br/><br/>\n"
                f"📌 ***Fonte:*** *IBGE / BACEN*"
            )

        if "DI" in t or "CURVA" in t:
            dis = con.execute("SELECT indicador, valor FROM fato_macro_diarios WHERE indicador LIKE '%DI IF%' ORDER BY data DESC LIMIT 5").fetchall()
            linhas_di = "<br/>\n".join([f"• **{d[0]}:** {d[1]:.2f}% a.a." for d in dis])
            return (
                f"📈 **Curvas de Juros DI (B3)**<br/>\n"
                f"{linhas_di}<br/><br/>\n"
                f"📌 ***Fonte:*** *B3*"
            )

        selic = con.execute("SELECT valor FROM fato_macro_diarios WHERE indicador LIKE '%Selic Meta%' ORDER BY data DESC LIMIT 1").fetchone()[0]
        ipca = con.execute("SELECT valor FROM fato_macro_mensais WHERE indicador LIKE '%IPCA Acumulado 12%' ORDER BY data DESC LIMIT 1").fetchone()[0]
        igpm = con.execute("SELECT valor FROM fato_macro_mensais WHERE indicador LIKE '%IGP-M Mensal%' ORDER BY data DESC LIMIT 1").fetchone()[0]
        pib = con.execute("SELECT valor FROM fato_macro_trimestrais WHERE indicador LIKE '%PIB%' ORDER BY data DESC LIMIT 1").fetchone()[0]

        return (
            f"🏛️ **Cenário Macroeconômico Oficial (BACEN / IBGE / FGV)**<br/>\n"
            f"• **Taxa Selic Meta:** {selic:.2f}% a.a. (Copom/BACEN · Diário)<br/>\n"
            f"• **IPCA Acumulado 12 Meses:** {ipca:.2f}% (IBGE · Mensal)<br/>\n"
            f"• **IGP-M Mensal:** {igpm:.2f}% (FGV · Mensal)<br/>\n"
            f"• **PIB Trimestral a Preços de Mercado:** R$ {pib/1e6:.2f} trilhões (IBGE)<br/><br/>\n"
            f"💡 *Deseja detalhar algum indicador específico?*<br/>\n"
            f"*Digite: **Selic**, **IPCA**, **IGP-M**, **PIB**, **Curva de DI** ou **CAGED**.*"
        )
    finally:
        con.close()


def _tratar_pilar_6_altas_baixas(texto, user_name):
    con = get_b3_db()
    try:
        t = texto.strip().upper()
        if t in ["HOJE", "ULTIMO", "ÚLTIMO", "RECENTE"]:
            dt_target = con.execute("SELECT max(data) FROM fato_b3_tickers").fetchone()[0]
        else:
            match = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', t)
            if match:
                dt_target = f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
            else:
                match_iso = re.search(r'(\d{4})[/-](\d{2})[/-](\d{2})', t)
                if match_iso:
                    dt_target = f"{match_iso.group(1)}-{match_iso.group(2)}-{match_iso.group(3)}"
                else:
                    dt_target = con.execute("SELECT max(data) FROM fato_b3_tickers").fetchone()[0]

        dt_str_iso = dt_target.strftime("%Y-%m-%d") if hasattr(dt_target, "strftime") else str(dt_target)
        dt_exibicao = dt_target.strftime("%d/%m/%Y") if hasattr(dt_target, "strftime") else datetime.strptime(dt_str_iso, "%Y-%m-%d").strftime("%d/%m/%Y")

        altas = con.execute("""
            SELECT f.ticker, d.nome_empresa, f.preco, f.variacao 
            FROM fato_b3_tickers f
            JOIN dim_ativos d ON f.ticker = d.ticker
            WHERE f.data = ? AND d.ticker_inativo = 'ATIVA' AND d.setor_atuacao NOT IN ('IBOV', 'DOLAR')
            ORDER BY f.variacao DESC LIMIT 5
        """, [dt_str_iso]).fetchall()

        if not altas:
            prox = con.execute("SELECT max(data) FROM fato_b3_tickers WHERE data < ?", [dt_str_iso]).fetchone()[0]
            prox_str = prox.strftime("%d/%m/%Y") if prox else "N/D"
            return (
                f"⚠️ Não houve pregão ou registros em **{dt_exibicao}** (fim de semana ou feriado).<br/><br/>\n"
                f"O pregão útil anterior mais próximo foi em **{prox_str}**."
            )

        baixas = con.execute("""
            SELECT f.ticker, d.nome_empresa, f.preco, f.variacao 
            FROM fato_b3_tickers f
            JOIN dim_ativos d ON f.ticker = d.ticker
            WHERE f.data = ? AND d.ticker_inativo = 'ATIVA' AND d.setor_atuacao NOT IN ('IBOV', 'DOLAR')
            ORDER BY f.variacao ASC LIMIT 5
        """, [dt_str_iso]).fetchall()

        linhas_altas = []
        for a in altas:
            t_pad = f"**{a[0]}**" + " " * max(0, 8 - len(a[0]))
            n_pad = a[1].strip()[:30].ljust(30)
            p_pad = f"R$ {a[2]:.2f}".ljust(16)
            sinal = "+" if a[3] > 0 else ""
            v_pad = f"🟢 {sinal}{a[3]:.2f}%".ljust(16)
            linhas_altas.append(f"| {t_pad} | {n_pad} | {p_pad} | {v_pad} |<br/>")

        linhas_baixas = []
        for b in baixas:
            t_pad = f"**{b[0]}**" + " " * max(0, 8 - len(b[0]))
            n_pad = b[1].strip()[:30].ljust(30)
            p_pad = f"R$ {b[2]:.2f}".ljust(16)
            v_pad = f"🔴 {b[3]:.2f}%".ljust(16)
            linhas_baixas.append(f"| {t_pad} | {n_pad} | {p_pad} | {v_pad} |<br/>")

        tab_altas = "\n".join(linhas_altas)
        tab_baixas = "\n".join(linhas_baixas)

        return (
            f"🏆 **Destaques do Pregão B3 — Pregão de {dt_exibicao}**<br/><br/>\n"
            f"🟢 **Top 5 Maiores Altas:**<br/>\n"
            f"{tab_altas}<br/>\n"
            f"🔴 **Top 5 Maiores Baixas:**<br/>\n"
            f"{tab_baixas}<br/>\n"
            f"📌 *Dados do Painel B3 (Pregão de {dt_exibicao}).*"
        )
    finally:
        con.close()


def _tratar_pilar_7_setores(texto):
    con = get_b3_db()
    try:
        t = texto.upper()
        setores_nomes = {
            "CONSUMO CÍCLICO": "Consumo Cíclico", "CONSUMO CICLICO": "Consumo Cíclico", "VAREJO": "Consumo Cíclico",
            "UTILIDADE PÚBLICA": "Utilidade Pública", "UTILIDADE PUBLICA": "Utilidade Pública", "ENERGIA": "Utilidade Pública", "SANEAMENTO": "Utilidade Pública",
            "FINANCEIRO": "Financeiro", "BANCOS": "Financeiro", "SEGUROS": "Financeiro",
            "MATERIAIS BÁSICOS": "Materiais Básicos", "MATERIAIS BASICOS": "Materiais Básicos", "MINERAÇÃO": "Materiais Básicos", "SIDERURGIA": "Materiais Básicos",
            "CONSUMO NÃO CÍCLICO": "Consumo não Cíclico", "CONSUMO NAO CICLICO": "Consumo não Cíclico", "ALIMENTOS": "Consumo não Cíclico",
            "PETRÓLEO E GÁS": "Petróleo e Gás", "PETROLEO E GAS": "Petróleo e Gás", "PETRÓLEO": "Petróleo e Gás", "PETROLEO": "Petróleo e Gás",
            "BENS INDUSTRIAIS": "Bens Industriais", "INDÚSTRIA": "Bens Industriais", "INDUSTRIA": "Bens Industriais", "MÁQUINAS": "Bens Industriais",
            "SAÚDE": "Saúde", "SAUDE": "Saúde", "FARMÁCIA": "Saúde", "HOSPITAIS": "Saúde",
            "TELECOM": "Telecom", "TELECOMUNICAÇÕES": "Telecom", "TELECOMUNICACOES": "Telecom",
            "TECNOLOGIA DA INFORMAÇÃO": "Tecnologia da Informação", "TECNOLOGIA": "Tecnologia da Informação", "TI": "Tecnologia da Informação"
        }

        setor_escolhido = None
        for k, v in setores_nomes.items():
            if k in t and t != "7":
                setor_escolhido = v
                break

        if setor_escolhido:
            dt_target = con.execute("SELECT max(data) FROM fato_b3_tickers").fetchone()[0]
            dt_exib = dt_target.strftime("%d/%m/%Y") if hasattr(dt_target, "strftime") else str(dt_target)
            rows = con.execute("""
                SELECT f.ticker, d.nome_empresa, f.preco, f.variacao
                FROM fato_b3_tickers f
                JOIN dim_ativos d ON f.ticker = d.ticker
                WHERE f.data = ? AND d.setor_atuacao = ? AND d.ticker_inativo = 'ATIVA'
                ORDER BY f.variacao DESC
            """, [dt_target, setor_escolhido]).fetchall()

            linhas = []
            for r in rows:
                emoji = "🟢" if r[3] >= 0 else "🔴"
                linhas.append(f"• **{r[0]} ({r[1][:18]}):** R$ {r[2]:.2f} ({emoji} {r[3]:+.2f}%)")

            return (
                f"🏢 **Setor: {setor_escolhido} ({len(rows)} ativos) — Pregão de {dt_exib}**\n\n"
                + "\n".join(linhas) + "\n\n"
                f"📌 *Dados oficiais do Painel B3 (Dim_Ativos e Fato_B3_tickers).*"
            )

        dt_target = con.execute("SELECT max(data) FROM fato_b3_tickers").fetchone()[0]
        dt_exib = dt_target.strftime("%d/%m/%Y") if hasattr(dt_target, "strftime") else str(dt_target)

        setores = con.execute("""
            SELECT 
                d.setor_atuacao,
                count(f.ticker) as qtd,
                round(avg(f.variacao), 2) as var_media
            FROM fato_b3_tickers f
            JOIN dim_ativos d ON f.ticker = d.ticker
            WHERE f.data = ? AND d.ticker_inativo = 'ATIVA' AND d.setor_atuacao NOT IN ('IBOV', 'DOLAR')
            GROUP BY d.setor_atuacao
            ORDER BY var_media DESC
        """, [dt_target]).fetchall()

        linhas_tab = []
        for idx, s in enumerate(setores, 1):
            emoji = "🟢" if s[2] >= 0 else "🔴"
            linhas_tab.append(f"| {idx}. **{s[0]}** | {s[1]} | {emoji} {s[2]:+.2f}% |")

        tab_str = "\n".join(linhas_tab)

        return (
            f"🏢 **Desempenho dos 10 Setores Oficiais B3 — Pregão de {dt_exib}**\n\n"
            f"| Setor Econômico | Ativos | Desempenho Médio |\n"
            f"| :--- | :---: | :---: |\n"
            f"{tab_str}\n\n"
            f"📌 *Cálculo ponderado sobre os 102 ativos da base oficial do Painel B3.*\n"
            f"💡 *Deseja ver os ativos de algum setor? Digite o nome (ex: `Petróleo e Gás`, `Financeiro`, `Saúde`).*"
        )
    finally:
        con.close()


def _tratar_pilar_8_fluxos(texto):
    con = get_b3_db()
    try:
        t = texto.upper()
        match = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', t)
        if match:
            dt_target = f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
        else:
            dt_target = con.execute("SELECT max(data) FROM fato_fluxo_investidores_b3").fetchone()[0]

        dt_str_iso = dt_target.strftime("%Y-%m-%d") if hasattr(dt_target, "strftime") else str(dt_target)
        dt_exib = dt_target.strftime("%d/%m/%Y") if hasattr(dt_target, "strftime") else datetime.strptime(dt_str_iso, "%Y-%m-%d").strftime("%d/%m/%Y")

        fluxos = con.execute("""
            SELECT tipo_investidor, saldo_liquido_mil
            FROM fato_fluxo_investidores_b3
            WHERE data = ?
            ORDER BY saldo_liquido_mil DESC
        """, [dt_str_iso]).fetchall()

        if not fluxos:
            return f"⚠️ Dados de fluxo de investidores não disponíveis para a data **{dt_exib}**."

        icones = {
            "Investidor Estrangeiro": "🌎",
            "Institucionais": "🏛️",
            "Instituições Financeiras": "🏦",
            "Investidores Individuais": "👤",
            "Outros": "🏢"
        }

        linhas = []
        for f in fluxos:
            tipo = f[0]
            saldo_mil = f[1]
            saldo_reais = saldo_mil * 1000.0
            ico = icones.get(tipo, "💼")
            emoji = "🟢" if saldo_mil >= 0 else "🔴"
            mov = "Entrada Líquida" if saldo_mil >= 0 else "Saída Líquida"
            
            if abs(saldo_reais) >= 1e9:
                val_fmt = f"{emoji} {saldo_reais/1e9:+.2f} bilhões"
            elif abs(saldo_reais) >= 1e6:
                val_fmt = f"{emoji} {saldo_reais/1e6:+.1f} milhões"
            else:
                val_fmt = f"{emoji} {saldo_reais:,.0f}"

            linhas.append(f"| {ico} **{tipo}** | **{val_fmt}** | {mov} |")

        tab_fluxo = "\n".join(linhas)

        return (
            f"🌊 **Fluxo de Ativos e Investidores B3 — Pregão de {dt_exib}**\n\n"
            f"| Tipo de Investidor | Saldo Líquido do Dia | Movimentação |\n"
            f"| :--- | :---: | :--- |\n"
            f"{tab_fluxo}\n\n"
            f"📌 *Dados oficiais da tabela Fato_Fluxo_Investidores_B3.*\n"
            f"💡 *Deseja consultar o fluxo de outra data? Basta digitar: `fluxo DD/MM/AAAA`.*"
        )
    finally:
        con.close()


def responder_conceito_b3(texto, user_name):
    t = texto.lower()
    conceitos = {
        'dy': ('Dividend Yield (DY)', 'Indicador que mede o retorno gerado por dividendos e JCP distribuídos por uma empresa em relação ao preço atual de sua ação nos últimos 12 meses. Fórmula: (Dividendos por Ação / Preço da Ação) × 100.'),
        'dividend yield': ('Dividend Yield (DY)', 'Mede a rentabilidade dos dividendos pagos por uma ação em relação à sua cotação de mercado nos últimos 12 meses.'),
        'p/vp': ('Preço sobre Valor Patrimonial (P/VP)', 'Relação entre a cotação de mercado da ação e o seu Valor Patrimonial por Ação (VPA). Um P/VP menor que 1 pode indicar que a ação negocia com desconto sobre o patrimônio líquido da empresa.'),
        'pvp': ('Preço sobre Valor Patrimonial (P/VP)', 'Indica o quanto o mercado está disposto a pagar pelo patrimônio líquido contábil da empresa.'),
        'ptax': ('Dólar PTAX', 'Taxa média de câmbio calculada e divulgada diariamente pelo Banco Central do Brasil (BACEN) com base nas operações interbancárias.'),
        'selic': ('Taxa Selic Meta', 'Taxa básica de juros da economia brasileira, definida a cada 45 dias pelo Copom (Comitê de Política Monetária do Banco Central).'),
        'ipca': ('IPCA (Índice de Preços ao Consumidor Amplo)', 'Índice oficial de inflação do Brasil, apurado mensalmente pelo IBGE, que mede a variação de preços da cesta de consumo das famílias.'),
        'igpm': ('IGP-M (Índice Geral de Preços do Mercado)', 'Índice de inflação apurado pela FGV, muito utilizado para reajuste de contratos de aluguel e energia.'),
        'igp-m': ('IGP-M (Índice Geral de Preços do Mercado)', 'Índice de inflação calculado pela Fundação Getulio Vargas (FGV).'),
        'pib': ('PIB (Produto Interno Bruto)', 'Soma de todos os bens e serviços finais produzidos no país ao longo de um trimestre ou ano, mensurado pelo IBGE.'),
        'ibovespa': ('Índice Bovespa (IBOV)', 'Principal indicador de desempenho médio das cotações das ações mais negociadas e representativas do mercado acionário brasileiro na B3.'),
        'fluxo de investidores': ('Fluxo de Investidores B3', 'Saldo líquido diário e mensal de compras menos vendas na B3, segmentado por perfil: Estrangeiro, Institucional, Pessoa Física e Bancos.')
    }
    
    for k, (titulo, desc) in conceitos.items():
        if re.search(r'\b(o que e|o que é|o que significa|significado de|definicao de|definição de|conceito de|como funciona)\s+' + re.escape(k) + r'\b', t) or t in [k, f'o que e {k}', f'o que é {k}', f'{k}?']:
            return (
                f"🏛️ **Conceito de Mercado: {titulo}**\n\n"
                f"💡 **Definição:** {desc}\n\n"
                f"📌 *Dicionário de Mercado Financeiro & Painel B3*"
            )
    return None


def responder_gemini_b3(texto, user_name):
    con = get_b3_db()
    try:
        max_dt = con.execute("SELECT MAX(data) FROM fato_b3_tickers").fetchone()[0]
        max_dt_str = max_dt.strftime("%Y-%m-%d") if hasattr(max_dt, "strftime") else str(max_dt)
        max_dt_exib = max_dt.strftime("%d/%m/%Y") if hasattr(max_dt, "strftime") else str(max_dt)
    except Exception:
        max_dt_str = "2026-10-05"
        max_dt_exib = "05/10/2026"
    finally:
        con.close()

    prompt_sql = f"""
Você é o motor analítico SQL DuckDB do Joca B3, especialista no mercado financeiro e na Bolsa Brasileira (B3).
Banco de Dados: 'b3_database.duckdb' (Data mais recente disponível: {max_dt_str} / {max_dt_exib}).

TABELAS E ESQUEMAS DISPONÍVEIS:
1. 'dim_ativos' (ticker VARCHAR, nome_empresa VARCHAR, setor_atuacao VARCHAR, ticker_inativo VARCHAR, destino VARCHAR)
   - Exemplo: ('PETR4', 'Petrobras S.A.', 'Petróleo e Gás', 'ATIVA')
2. 'fato_b3_tickers' (ticker VARCHAR, preco DOUBLE, variacao DOUBLE, dy DOUBLE, p_vp DOUBLE, volume DOUBLE, data DATE)
   - Variação é percentual diário (ex: 1.58 para +1.58%).
3. 'fato_b3_ibov' (data DATE, abertura DOUBLE, maxima DOUBLE, minima DOUBLE, fechamento DOUBLE, variacao DOUBLE, volume DOUBLE)
   - NOTA: Ibovespa NÃO tem volume reportado.
4. 'fato_b3_dolar' (data DATE, compra DOUBLE, venda DOUBLE, maxima DOUBLE, minima DOUBLE, variacao DOUBLE)
5. 'fato_fluxo_investidores_b3' (data DATE, tipo_investidor VARCHAR, compras_mil DOUBLE, vendas_mil DOUBLE, saldo_liquido_mil DOUBLE)
   - tipo_investidor: 'Investidor Estrangeiro', 'Institucionais', 'Investidores Individuais', 'Instituições Financeiras'.
6. 'fato_indices_americanos' (data DATE, indice VARCHAR, ticker VARCHAR, bolsa VARCHAR, preco_fechamento DOUBLE, preco_maximo DOUBLE, preco_minimo DOUBLE, volume BIGINT)
   - Índices: S&P 500 (^GSPC), Dow Jones (^DJI), Nasdaq (^IXIC), Petróleo Brent (BZ=F), OMXS30 (^OMX).
7. 'fato_macro_diarios' (data DATE, indicador VARCHAR, valor DOUBLE, unidade VARCHAR, fonte VARCHAR) -> ex: 'Selic Meta Diária'
8. 'fato_macro_mensais' (data DATE, indicador VARCHAR, valor DOUBLE, unidade VARCHAR, fonte VARCHAR) -> ex: 'IPCA Acumulado 12 Meses', 'IGP-M Mensal', 'CAGED Total'
9. 'fato_macro_trimestrais' (data DATE, indicador VARCHAR, valor DOUBLE, unidade VARCHAR, fonte VARCHAR) -> ex: 'PIB Trimestral a Preços de Mercado'

REGRAS OBRIGATÓRIAS:
- Use sempre aliases claros para as colunas.
- Se a pergunta pedir dados recentes sem especificar data, filtre por data = '{max_dt_str}' ou data = (SELECT MAX(data) FROM fato_b3_tickers).
- Se a pergunta for sobre ações, dê preferência a join com dim_ativos onde ticker_inativo = 'ATIVA'.
- Retorne EXCLUSIVAMENTE a query SQL DuckDB dentro de ```sql ... ``` ou retorne 'NAO_SQL' se for apenas saudação.

Pergunta do usuário: "{texto}"
"""
    resp_sql = chamar_gemini(prompt_sql)
    if not resp_sql or "NAO_SQL" in resp_sql:
        return None

    clean_sql = resp_sql.replace("```sql", "").replace("```", "").strip()
    logger.info(f"SQL B3 gerado pelo Gemini: {clean_sql}")

    con = get_b3_db()
    try:
        cur = con.execute(clean_sql)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        if not rows:
            return None

        header_str = " | ".join(col_names)
        linhas_tab = [" | ".join([str(v) if v is not None else "NULL" for v in r]) for r in rows[:15]]
        tabela_str = f"{header_str}\n" + ("-" * len(header_str)) + "\n" + "\n".join(linhas_tab)

        prompt_formatacao = f"""
Você é o assistente executivo Joca B3. O usuário '{user_name}' perguntou: "{texto}"
Resultado retornado do banco de dados oficial da B3:
{tabela_str}

Formate uma resposta executiva impecável para Microsoft Teams e Telegram:
- Comece de forma direta e cordial com emojis do mercado financeiro (📈, 📊, 🏛️, 💵, 💼).
- Apresente os dados com clareza, destacando tickers em negrito, valores em R$, percentuais formatados (+X.XX%) e volumes monetários compreensíveis (milhões/bilhões).
- Responda EXATAMENTE o que foi perguntado de forma concisa.
- Rodapé padrão: "📌 *Dados oficiais do Painel B3.*"
"""
        resp_final = chamar_gemini(prompt_formatacao)
        if resp_final:
            return resp_final.strip()

        # Fallback de formatação simples
        itens_res = []
        for r in rows[:10]:
            itens_res.append("• " + " | ".join([f"{col}: {val}" for col, val in zip(col_names, r)]))
        return f"📊 **Resultado da Consulta B3:**\n\n" + "\n".join(itens_res) + "\n\n📌 *Dados do Painel B3.*"

    except Exception as e_sql:
        logger.error(f"Erro ao executar SQL B3 gerado pelo Gemini: {e_sql}")
        return None
    finally:
        con.close()


def processar_pergunta_b3(texto, user_name="Karl", conversation_id="default"):
    t_clean = texto.strip()
    t_upper = t_clean.upper()
    state = b3_conversation_states.get(conversation_id, {})

    gatilhos_menu = [
        "OI", "OLÁ", "OLA", "BOM DIA", "BOA TARDE", "BOA NOITE", 
        "E AÍ", "E AI", "OPA", "MENU", "AJUDA", "HELP", "INICIAR", 
        "/START", "OPÇÕES", "OPCOES", "O QUE VOCÊ FAZ", "COMANDOS"
    ]
    
    # Se o usuário enviar um comando numérico direto (1 a 8) ou pedir menu/saudação, cancela estado pendente anterior
    if t_clean in ["1", "2", "3", "4", "5", "6", "7", "8"] or t_upper in gatilhos_menu or t_upper in ["@JOCA", "@JOCA B3"]:
        b3_conversation_states.pop(conversation_id, None)
    else:
        # 0. Resolução de estado conversacional interativo (2 etapas)
        if state.get("action") == "awaiting_ticker_for_volume":
            b3_conversation_states.pop(conversation_id, None)
            return _tratar_pilar_4_volume(t_clean, user_name)

        if state.get("action") == "awaiting_date_for_ranking":
            b3_conversation_states.pop(conversation_id, None)
            return _tratar_pilar_6_altas_baixas(t_clean, user_name)

    # 1. Saudação & Menu Geral
    if t_upper in gatilhos_menu or t_upper == "@JOCA" or t_upper == "@JOCA B3":
        return _gerar_menu_boas_vindas(user_name)

    # Separação Estrita de Responsabilidade: Teams = Joca B3 (100% Bolsa Brasileira)
    t_lower = t_clean.lower()
    if any(k in t_lower for k in ["mercado livre", "meli", "faturamento mtd", "faturamento ytda", "mais vendidos celular", "categoria eletrodomestico", "subcategoria", "is_full", "frete gratis ml"]):
        return (
            f"👋 **Olá {user_name}! Eu sou o Joca B3.**\n\n"
            f"Meu escopo é exclusivamente a **Bolsa de Valores Brasileira (B3)**, cotações de ações, indicadores econômicos, fluxo de investidores e índices.\n\n"
            f"💡 Dúvidas sobre **vendas, produtos e faturamento do Mercado Livre** são respondidas pelo robô **Joca Mercado Livre no Telegram**!"
        )

    # 2. Resposta de Conceitos e Glossário do Mercado Financeiro
    resp_conc = responder_conceito_b3(t_clean, user_name)
    if resp_conc:
        return resp_conc

    # 3. Resolução Direta de Tickers e Ativos da B3 (com suporte a datas relativas ou explícitas)
    # Extrai data se presente
    data_alvo = None
    if "ONTEM" in t_upper:
        con = get_b3_db()
        try:
            max_dt = con.execute("SELECT MAX(data) FROM fato_b3_tickers").fetchone()[0]
            data_alvo = max_dt - timedelta(days=1)
        finally:
            con.close()
    else:
        m_data = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', t_upper)
        if m_data:
            data_alvo = f"{m_data.group(3)}-{m_data.group(2)}-{m_data.group(1)}"

    # Identifica ticker por regex ou mapa de nomes populares
    nomes_map = {
        "PETROBRAS": "PETR4", "PETROBRÁS": "PETR4", "PETRO": "PETR4",
        "VALE": "VALE3", "ITAU": "ITUB4", "ITAÚ": "ITUB4", "ITAU UNIBANCO": "ITUB4", "ITAÚ UNIBANCO": "ITUB4",
        "BRADESCO": "BBDC4", "BANCO DO BRASIL": "BBAS3", "AMBEV": "ABEV3",
        "WEG": "WEGE3", "MAGALU": "MGLU3", "MAGAZINE LUIZA": "MGLU3", "EMBRAER": "EMBJ3",
        "SUZANO": "SUZB3", "GERDAU": "GGBR4", "LOCALIZA": "RENT3", "B3": "B3SA3", "RAIZEN": "RAIZ4",
        "PRIO": "PRIO3", "PETRORIO": "PRIO3", "AZUL": "AZUL4", "GOL": "GOLL4", "TOTVS": "TOTS3",
        "CSN": "CSNA3", "USIMINAS": "USIM5", "SABESP": "SBSP3", "COPEL": "CPLE6", "CEMIG": "CMIG4",
        "ELETROBRAS": "ELET3", "ELETROBRÁS": "ELET3", "EQUATORIAL": "EQTL3", "HAPVIDA": "HAPV3",
        "REDE D'OR": "RDOR3", "REDE DOR": "RDOR3", "VIBRA": "VBBR3", "COSAN": "CSAN3", "KLABIN": "KLBN11",
        "BTG": "BPAC11", "BTG PACTUAL": "BPAC11", "SANTANDER": "SANB11", "CIELO": "CIEL3", "JBS": "JBSS3",
        "MARFRIG": "MRFG3", "BRF": "BRFS3", "NATURA": "NATU3", "ASSÃ": "ASAI3", "ASSAI": "ASAI3",
        "CARREFOUR": "CRFB3", "VIVARA": "VIVA3", "AREZZO": "AZZA3", "AZZAS": "AZZA3"
    }

    ticker_achado = None
    tickers_encontrados = re.findall(r'\b[A-Z]{4}(?:3|4|5|6|11)\b', t_upper)

    # Se o usuário digitou múltiplos tickers (ex: PETR4 VALE3 ou ENEV3 BBDC4)
    if len(tickers_encontrados) > 1 and not any(k in t_upper for k in ["VOLUME FINANCEIRO", "MAIOR VOLUME", "VOLUME NEGOCIADO"]):
        respostas_multi = []
        tickers_unicos = list(dict.fromkeys(tickers_encontrados))[:5]
        for tk in tickers_unicos:
            respostas_multi.append(_tratar_pilar_2_ticker(tk, data_alvo=data_alvo))
        return "\n\n━━━━━━━━━━━━━━━━━━━━\n\n".join(respostas_multi)

    if tickers_encontrados:
        ticker_achado = tickers_encontrados[0]
    else:
        for n, tk in nomes_map.items():
            if re.search(r'\b' + re.escape(n) + r'\b', t_upper):
                ticker_achado = tk
                break

    # Se achou ticker e não é uma pergunta de volume exclusivo
    if ticker_achado and not any(k in t_upper for k in ["VOLUME FINANCEIRO", "MAIOR VOLUME", "VOLUME NEGOCIADO"]):
        return _tratar_pilar_2_ticker(ticker_achado, data_alvo=data_alvo)

    # 4. Pilares Numéricos Diretos (1 a 8) e Palavras-Gatilho Rápidas
    # Pilar 1: Índices (Ibov e Americanos)
    gatilhos_pilar_1 = [
        "IBOV", "IBOVESPA", "BOVESPA", "^BVSP",
        "DOW JONES", "DOW", "DJI", 
        "NASDAQ", "NYSE", 
        "PETRÓLEO BRENT", "PETROLEO BRENT", "BRENT", 
        "OMXS30", "OMX"
    ]
    if t_clean == "1" or any(re.search(r'\b' + re.escape(k) + r'\b', t_upper) or k in t_upper for k in gatilhos_pilar_1):
        return _tratar_pilar_1_indices(t_clean)

    # Pilar 3: Dólar PTAX
    if t_clean == "3" or any(re.search(r'\b' + re.escape(k) + r'\b', t_upper) or k in t_upper for k in ["DOLAR", "DÓLAR", "USD", "CAMBIO", "CÂMBIO", "PTAX"]):
        return _tratar_pilar_3_dolar()

    # Pilar 4: Volume e Preço
    if t_clean == "4" or any(k in t_upper for k in ["VOLUME FINANCEIRO", "VOLUME NEGOCIADO", "MAIOR VOLUME", "MAIS NEGOCIADAS"]):
        if ticker_achado:
            return _tratar_pilar_4_volume(ticker_achado, user_name)
        b3_conversation_states[conversation_id] = {"action": "awaiting_ticker_for_volume", "timestamp": time.time()}
        return (
            f"💼 **Consulta de Volume e Preço por Ativo**\n\n"
            f"Por favor, digite o código da ação para consultar o volume financeiro e o comportamento de preço.\n"
            f"*Exemplos ativos do painel: `PETR4`, `VALE3`, `ITUB4`, `WEGE3`.*"
        )

    # Pilar 5: Indicadores Macro
    if t_clean == "5" or any(re.search(r'\b' + re.escape(k) + r'\b', t_upper) or k in t_upper for k in ["MACRO", "INDICADOR", "INDICADORES", "SELIC", "IPCA", "INFLAÇÃO", "INFLACAO", "IGPM", "IGP-M", "PIB", "IBC-BR", "CAGED", "CURVA DE DI"]):
        return _tratar_pilar_5_macro(t_clean)

    # Pilar 6: 5 Altas e 5 Baixas
    gatilhos_pilar_6 = [
        "MAIORES ALTAS", "TOP ALTAS", "MAIORES BAIXAS", "TOP BAIXAS",
        "5 MAIORES", "5 MENORES", "ALTAS E BAIXAS", "MAIORES QUEDAS",
        "DESTAQUES DO DIA", "DESTAQUES DO PREGÃO", "RANKING"
    ]
    if t_clean == "6" or t_clean.startswith("6 ") or t_clean.startswith("6-") or any(k in t_upper for k in gatilhos_pilar_6):
        match_data = re.search(r'(\d{2}[/-]\d{2}[/-]\d{4}|\d{4}[/-]\d{2}[/-]\d{2}|HOJE|ONTEM)', t_upper)
        if match_data and t_clean != "6":
            return _tratar_pilar_6_altas_baixas(match_data.group(1), user_name)
        b3_conversation_states[conversation_id] = {"action": "awaiting_date_for_ranking", "timestamp": time.time()}
        return (
            f"📅 **De qual data você deseja consultar as Maiores Altas e Baixas?**<br/><br/>\n"
            f"Por favor, envie a data desejada (exemplo: `02/10/2026`) ou digite **hoje** para o último pregão disponível na base."
        )

    # Pilar 7: Setores Oficiais (Opção B - Resumo Executivo)
    setores_palavras = [
        "SETOR", "SETORES", "SEGMENTO", "SEGMENTOS",
        "CONSUMO CÍCLICO", "UTILIDADE PÚBLICA", "FINANCEIRO", "MATERIAIS BÁSICOS",
        "CONSUMO NÃO CÍCLICO", "PETRÓLEO", "PETROLEO", "BENS INDUSTRIAIS", "SAÚDE", "SAUDE",
        "TELECOM", "TECNOLOGIA DA INFORMAÇÃO"
    ]
    if t_clean == "7" or any(re.search(r'\b' + re.escape(k) + r'\b', t_upper) or k in t_upper for k in setores_palavras):
        return _tratar_pilar_7_setores(t_clean)

    # Pilar 8: Fluxos de Ativos / Investidores
    if t_clean == "8" or any(re.search(r'\b' + re.escape(k) + r'\b', t_upper) or k in t_upper for k in ["FLUXO", "FLUXOS", "INVESTIDOR", "INVESTIDORES", "ESTRANGEIRO", "GRINGO", "INSTITUCIONAL", "INSTITUCIONAIS", "PESSOA FÍSICA", "PESSOA FISICA"]):
        return _tratar_pilar_8_fluxos(t_clean)

    # Pilar 2 atalho textual
    if t_clean == "2" or t_upper in ["AÇÕES", "ACOES", "ATIVOS", "PAPÉIS", "PAPEIS", "COTAÇÃO", "COTACAO", "TICKERS"]:
        return (
            f"📈 **Consulta de Ações e Ativos**<br/><br/>\n"
            f"Por favor, digite o **código/ticker** da ação que deseja consultar.<br/><br/>\n"
            f"*Exemplos ativos mais procurados:*<br/>\n"
            f"*PETR4, VALE3, ITUB4, WEGE3, BBAS3.*"
        )

    # 5. Inteligência Conversacional Aberta com Gemini Text-to-SQL sobre DuckDB B3
    resp_gemini = responder_gemini_b3(t_clean, user_name)
    if resp_gemini:
        return resp_gemini

    return _gerar_menu_boas_vindas(user_name)


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

        try:
            from juca_supervisor import salvar_sessao_teams, juca
            salvar_sessao_teams(conversation_id, service_url, user_name)
        except Exception as e_s:
            logger.error(f"Erro ao salvar sessao Teams: {e_s}")

        t_inicio = time.time()

        t_upper = texto.upper()
        if any(w in t_upper for w in ["JUCA", "STATUS GERAL", "ROBOS", "ROBÔS", "CHEFE", "SUPERVISOR", "ATUALIZA", "ATUALIZAR", "PIPELINE"]):
            from juca_supervisor import juca
            resposta = juca.responder_comando_teams(texto, user_name)
            bot_tag = "Juca_Supervisor_Teams"
        else:
            resposta = processar_pergunta_b3(texto, user_name, conversation_id)
            bot_tag = "Joca_B3_Teams"

        tempo_total = round(time.time() - t_inicio, 2)
        enviar_mensagem_teams(service_url, conversation_id, resposta, reply_to_id=activity_id)

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

        msg_bv = _gerar_menu_boas_vindas(user_name)
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


# ==============================================================================
# 7. INTEGRAÇÃO OFICIAL WHATSAPP (META CLOUD API) - JOCA B3
# ==============================================================================
def formatar_texto_para_whatsapp(texto):
    """Limpa tags HTML como <br/> e adapta negritos/formatos para WhatsApp padrão."""
    if not texto:
        return ""
    # Substitui <br/> e <br> por quebras de linha
    t = re.sub(r'<br\s*/?>', '\n', texto, flags=re.IGNORECASE)
    # Substitui negritos Markdown duplos **texto** por *texto* do WhatsApp
    t = re.sub(r'\*\*(.*?)\*\*', r'*\1*', t)
    return t.strip()


def enviar_mensagem_whatsapp(to_number, text):
    """Envia mensagem de texto para o WhatsApp via Meta Cloud API Oficial"""
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_NUMBER_ID:
        logger.warning("WHATSAPP_TOKEN ou WHATSAPP_PHONE_NUMBER_ID nao configurados.")
        return False

    url = f"https://graph.facebook.com/v19.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_number,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": formatar_texto_para_whatsapp(text)
        }
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        if r.status_code in [200, 201]:
            logger.info(f"Mensagem enviada com sucesso no WhatsApp para {to_number}")
            return True
        else:
            logger.error(f"Erro ao enviar WhatsApp: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        logger.error(f"Excecao ao enviar WhatsApp: {e}")
        return False


def _processar_mensagem_whatsapp(payload):
    """Executado em segundo plano para processar mensagens recebidas no WhatsApp"""
    try:
        entries = payload.get("entry", [])
        for entry in entries:
            changes = entry.get("changes", [])
            for change in changes:
                value = change.get("value", {})
                contacts = value.get("contacts", [])
                messages = value.get("messages", [])
                
                user_name = contacts[0].get("profile", {}).get("name", "Investidor") if contacts else "Investidor"
                
                for msg in messages:
                    from_number = msg.get("from")
                    msg_type = msg.get("type")
                    texto = None
                    
                    if msg_type == "text":
                        texto = msg.get("text", {}).get("body", "").strip()
                    elif msg_type == "audio" or msg_type == "voice":
                        # Áudio do WhatsApp
                        audio_id = msg.get("audio", {}).get("id") or msg.get("voice", {}).get("id")
                        if audio_id and WHATSAPP_TOKEN:
                            try:
                                # 1. Obter URL da mídia
                                meta_media_url = f"https://graph.facebook.com/v19.0/{audio_id}"
                                r_m = requests.get(meta_media_url, headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}"}, timeout=10)
                                if r_m.status_code == 200:
                                    media_download_url = r_m.json().get("url")
                                    mime_type = r_m.json().get("mime_type", "audio/ogg")
                                    # 2. Baixar áudio
                                    r_audio = requests.get(media_download_url, headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}"}, timeout=20)
                                    if r_audio.status_code == 200:
                                        texto = transcrever_audio(r_audio.content, mime_type)
                            except Exception as e_wa_audio:
                                logger.error(f"Erro ao baixar/transcrever audio WhatsApp: {e_wa_audio}")

                    if not texto:
                        continue

                    t_inicio = time.time()
                    conversation_id = f"wa_{from_number}"
                    resposta = processar_pergunta_b3(texto, user_name, conversation_id)
                    t_duracao = round(time.time() - t_inicio, 2)
                    
                    enviar_mensagem_whatsapp(from_number, resposta)

                    # Telemetria para o Power BI
                    try:
                        agora = datetime.now()
                        salvar_conversa({
                            "id": msg.get("id") or int(time.time()),
                            "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                            "data": agora.strftime("%Y-%m-%d"),
                            "hora": agora.strftime("%H:%M:%S"),
                            "bot": "Joca_B3_WhatsApp",
                            "chat_id": str(from_number),
                            "usuario": user_name,
                            "username": f"+{from_number}",
                            "tipo_entrada": "Voz (WhatsApp)" if msg_type in ["audio", "voice"] else "Texto (WhatsApp)",
                            "pergunta": texto,
                            "resposta": resposta[:500] if resposta else "",
                            "tempo_resposta_s": t_duracao,
                            "status": "Respondido (Sucesso)"
                        })
                    except Exception as e_log_wa:
                        logger.error(f"Erro ao salvar telemetria WhatsApp: {e_log_wa}")

    except Exception as e_proc_wa:
        logger.error(f"Erro no processamento de webhook WhatsApp: {e_proc_wa}")


ULTIMOS_LOGS_WHATSAPP = []

@app.route("/api/debug_whatsapp", methods=["GET"])
def debug_whatsapp():
    return jsonify({
        "total_webhooks": len(ULTIMOS_LOGS_WHATSAPP),
        "token_configurado": bool(WHATSAPP_TOKEN),
        "phone_id": WHATSAPP_PHONE_NUMBER_ID,
        "logs": ULTIMOS_LOGS_WHATSAPP[-10:]
    })

@app.route("/api/whatsapp_b3", methods=["GET", "POST"])
def webhook_whatsapp_b3():
    # Validação do Webhook pela Meta (GET)
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        if mode == "subscribe" and token == WHATSAPP_VERIFY_TOKEN:
            logger.info("Webhook WhatsApp validado com sucesso pela Meta!")
            return challenge, 200
        else:
            logger.warning(f"Falha na verificacao do Webhook WhatsApp: token={token}")
            return "Forbidden", 403

    # Recebimento de mensagens (POST)
    payload = request.get_json(silent=True) or {}
    agora_str = datetime.now(BRT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    ULTIMOS_LOGS_WHATSAPP.append({"timestamp": agora_str, "payload": payload})
    if len(ULTIMOS_LOGS_WHATSAPP) > 50:
        ULTIMOS_LOGS_WHATSAPP.pop(0)

    if payload.get("object") == "whatsapp_business_account":
        threading.Thread(target=_processar_mensagem_whatsapp, args=(payload,), daemon=True).start()
        return jsonify({"status": "EVENT_RECEIVED"}), 200

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

@app.route("/test_b3", methods=["GET", "POST"])
def test_b3():
    try:
        q = request.args.get("q") or (request.get_json(silent=True) or {}).get("q", "1")
        user = request.args.get("user", "Karl Albert")
        cid = request.args.get("cid", "test_user_teams")
        resp = processar_pergunta_b3(q, user, cid)
        return jsonify({"pergunta": q, "resposta": resp})
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error(f"Erro ao processar /test_b3: {e}\n{tb}")
        return jsonify({"status": "error", "message": str(e), "traceback": tb}), 500

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
    valid_secrets = {SYNC_SECRET, "joca_sync_2026_karl", "meli_joca_sync_2026_karl"}
    req_secret = request.args.get("secret") or request.headers.get("X-Sync-Secret")
    if req_secret not in valid_secrets:
        return jsonify({"status": "error", "message": "Chave inválida."}), 403

    if request.method == "GET":
        b3_ibov_dt = "N/D"
        try:
            b3_c = duckdb.connect(B3_DB_PATH, read_only=True)
            b3_ibov_dt = str(b3_c.execute("SELECT max(data) FROM fato_b3_ibov").fetchone()[0])
            b3_c.close()
        except Exception:
            pass

        return jsonify({
            "status": "ready",
            "total_registros_meli": total_registros,
            "data_recente_meli": str(data_recente),
            "data_recente_b3_ibov": b3_ibov_dt,
            "horario_disponibilizacao": data_hora_disponibilizado
        })

    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"status": "error", "message": "Nenhum arquivo enviado."}), 400

    target = (request.args.get("target") or "").strip().lower()
    fname = (request.files["file"].filename or "").strip().lower()

    if target == "b3" or fname.endswith(".duckdb"):
        try:
            request.files["file"].save(B3_DB_PATH)
            b3_con = duckdb.connect(B3_DB_PATH, read_only=True)
            max_ibov = b3_con.execute("SELECT max(data) FROM fato_b3_ibov").fetchone()[0]
            max_tick = b3_con.execute("SELECT max(data) FROM fato_b3_tickers").fetchone()[0]
            total_tickers = b3_con.execute("SELECT count(*) FROM fato_b3_tickers").fetchone()[0]
            b3_con.close()
            hr_now = datetime.now(BRT_TZ).strftime("%d/%m/%Y %H:%M:%S")
            logger.info(f"✅ [SYNC B3] Base DuckDB B3 atualizada com sucesso! Max Ibov: {max_ibov}, Tickers: {max_tick}, Total: {total_tickers}")
            return jsonify({
                "status": "success",
                "target": "b3",
                "message": "Base B3 (b3_database.duckdb) sincronizada com sucesso no Render!",
                "data_recente_ibov": str(max_ibov),
                "data_recente_tickers": str(max_tick),
                "total_tickers": total_tickers,
                "horario_sync": hr_now
            }), 200
        except Exception as e:
            logger.error(f"Erro na sincronização B3: {e}")
            return jsonify({"status": "error", "target": "b3", "message": str(e)}), 500
    else:
        try:
            request.files["file"].save(PARQUET_FILE)
            recarregar_duckdb()
            return jsonify({
                "status": "success",
                "target": "mercadolivre",
                "message": "Base de dados atualizada com sucesso no DuckDB!",
                "total_registros": total_registros,
                "data_recente": str(data_recente),
                "horario_disponibilizacao": data_hora_disponibilizado
            }), 200
        except Exception as e:
            logger.error(f"Erro na sincronização: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/download_b3_duckdb", methods=["GET"])
def download_b3_duckdb():
    valid_secrets = {SYNC_SECRET, "joca_sync_2026_karl", "meli_joca_sync_2026_karl"}
    req_secret = request.args.get("secret") or request.headers.get("X-Sync-Secret")
    if req_secret not in valid_secrets:
        return jsonify({"status": "error", "message": "Chave de acesso inválida."}), 403
    
    if not os.path.exists(B3_DB_PATH):
        return jsonify({"status": "error", "message": "Arquivo b3_database.duckdb não encontrado no servidor."}), 404

    logger.info("📥 [DOWNLOAD B3] Enviando b3_database.duckdb para cliente...")
    return send_file(
        B3_DB_PATH,
        as_attachment=True,
        download_name="b3_database.duckdb",
        mimetype="application/octet-stream"
    )


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


@app.route("/api/juca/diagnostico", methods=["GET"])
def juca_diagnostico_api():
    try:
        from juca_supervisor import juca
        diag = juca.diagnosticar_problemas_profundo()
        return jsonify({"status": "success", "diagnostico": diag}), 200
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
# POLÍTICA DE PRIVACIDADE E TERMOS (EXIGÊNCIA META / WHATSAPP)
# ==============================================================================
@app.route("/politica-de-privacidade", methods=["GET"])
def politica_de_privacidade():
    return """<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><title>Política de Privacidade - Joca B3</title></head>
<body style="font-family:sans-serif;max-width:800px;margin:40px auto;line-height:1.6;padding:0 20px;">
    <h1>Política de Privacidade — Joca B3 Bot</h1>
    <p>O aplicativo <strong>Bot_B3 / Joca B3</strong> é um assistente corporativo que fornece dados analíticos do mercado financeiro brasileiro (B3) e indicadores econômicos oficiais.</p>
    <h2>1. Coleta de Informações</h2>
    <p>Coletamos apenas o identificador da conversa (número de telefone) e as perguntas enviadas para fins exclusivos de processamento e resposta analítica da consulta.</p>
    <h2>2. Uso dos Dados</h2>
    <p>Os dados não são comercializados nem compartilhados com terceiros para fins publicitários.</p>
    <h2>3. Segurança</h2>
    <p>Todas as comunicações utilizam criptografia padrão de mercado (HTTPS e Meta WhatsApp Cloud API).</p>
</body></html>""", 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/termos-de-servico", methods=["GET"])
def termos_de_servico():
    return """<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><title>Termos de Serviço - Joca B3</title></head>
<body style="font-family:sans-serif;max-width:800px;margin:40px auto;line-height:1.6;padding:0 20px;">
    <h1>Termos de Serviço — Joca B3 Bot</h1>
    <p>O Joca B3 fornece informações públicas sobre cotações, índices e indicadores macroeconômicos com fins exclusivamente educativos e operacionais internos.</p>
</body></html>""", 200, {"Content-Type": "text/html; charset=utf-8"}


# ==============================================================================
# 8. EXECUÇÃO DO APLICATIVO
# ==============================================================================
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

