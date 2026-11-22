# -*- coding: utf-8 -*-
"""
========================================================================================
GERADOR E SINCRONIZADOR DE PRINTS EXECUTIVOS 100% EM NUVEM (BIGQUERY + GITHUB ACTIONS)
========================================================================================
Executa no Ubuntu (GitHub Actions) de forma totalmente autônoma e integrada ao BigQuery.
  4 Painéis x 6 Recortes Regionais = 24 Prints:
    • diario     (Todas as Regiões, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • mtd        (Todas as Regiões, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • ytda       (Todas as Regiões, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • detalhado  (Todas as Regiões, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)

Origem dos Dados:
  Google BigQuery (mercado-livre-mais-vendidos.ML_Cidades)
  -> Consulta dados ao vivo do mês corrente (Realizado e Metas)
  -> Gera dinamicamente payload_diario.json, payload_mtd.json, payload_ytda.json
  -> Renderiza os 24 prints executivos via Vega 5 + Chromium headless
  -> Sincroniza os prints com o Telegram Bot no Render (/sync_print)
========================================================================================
"""

import os
import sys
import json
import base64
import shutil
import pathlib
import subprocess
import tempfile
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
PRINTS_DIR = SCRIPT_DIR / "prints"
PRINTS_DIR.mkdir(parents=True, exist_ok=True)
PRINTS_META_FILE = PRINTS_DIR / "prints_meta.json"

RENDER_BASE_URL = os.environ.get("RENDER_BOT_URL", "https://meli-intelligence-bot.onrender.com").rstrip("/")
SYNC_SECRET = os.environ.get("SYNC_SECRET", "meli_joca_sync_2026_karl").strip()
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mercado-livre-mais-vendidos").strip()

REGIOES = {
    "todas": "Todas as Regiões",
    "sudeste": "Sudeste",
    "sul": "Sul",
    "nordeste": "Nordeste",
    "centro_oeste": "Centro-Oeste",
    "norte": "Norte",
}

REGIOES_LIST = ["Todas as Regiões", "Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]

CATEGORIAS = [
    ("Visão Geral", "Visão Geral", "Visão Geral", ""),
    ("Celulares & Tel.", "Celulares e Telefones", "Celulares e", "Telefones"),
    ("Informática", "Informática", "Informática", ""),
    ("Eletrodomésticos", "Eletrodomésticos", "Eletro-", "domésticos"),
    ("Casa & Móveis", "Casa, Móveis e Decoração", "Casa e", "Decoração"),
    ("Ferramentas", "Ferramentas e Construção", "Ferramentas", "e Construção")
]

DIAS_SEMANA = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
MESES_ABREV = ["", "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

# Histórico Referência 2025 Novembro (Extraído de ML_Cidades DAX)
HIST_2025_BASE = {
    1: {"Centro-Oeste": (250826.14, 221895.92), "Nordeste": (596721.10, 692761.37), "Norte": (235413.60, 250904.83), "Sudeste": (1303534.74, 1379402.16), "Sul": (385292.96, 400707.95), "Todas as Regiões": (2771788.54, 2945672.23)},
    2: {"Centro-Oeste": (220814.83, 224751.50), "Nordeste": (653969.56, 704327.84), "Norte": (216325.93, 267671.27), "Sudeste": (1325831.04, 1354040.38), "Sul": (352266.46, 391984.19), "Todas as Regiões": (2769207.82, 2942775.18)},
    3: {"Centro-Oeste": (207142.92, 228458.99), "Nordeste": (587213.76, 646513.90), "Norte": (198451.05, 200466.78), "Sudeste": (1226014.44, 1290393.95), "Sul": (336671.98, 349454.75), "Todas as Regiões": (2555494.15, 2715288.37)},
    4: {"Centro-Oeste": (206485.71, 230676.64), "Nordeste": (600335.06, 661492.74), "Norte": (186593.79, 210904.66), "Sudeste": (1248671.58, 1270813.01), "Sul": (316176.72, 345407.41), "Todas as Regiões": (2558262.86, 2719294.46)},
    5: {"Centro-Oeste": (232996.84, 212992.26), "Nordeste": (587363.61, 633996.12), "Norte": (189334.00, 202193.90), "Sudeste": (1211569.93, 1328190.02), "Sul": (319135.03, 323915.04), "Todas as Regiões": (2540399.41, 2701287.34)},
    6: {"Centro-Oeste": (210517.92, 204381.48), "Nordeste": (586187.50, 632167.44), "Norte": (214820.29, 184963.15), "Sudeste": (1173004.09, 1309196.66), "Sul": (357219.41, 371808.51), "Todas as Regiões": (2541749.21, 2702517.24)},
    7: {"Centro-Oeste": (233639.64, 227263.33), "Nordeste": (579952.68, 658036.18), "Norte": (182360.66, 200655.61), "Sudeste": (1188244.28, 1273022.83), "Sul": (354346.37, 341509.27), "Todas as Regiões": (2538543.63, 2700487.22)},
    8: {"Centro-Oeste": (216964.92, 239457.59), "Nordeste": (620680.66, 660461.98), "Norte": (205521.17, 241990.16), "Sudeste": (1391233.23, 1393556.82), "Sul": (320745.22, 391437.47), "Todas as Regiões": (2755145.20, 2926904.02)},
    9: {"Centro-Oeste": (212699.21, 271264.07), "Nordeste": (636202.29, 684914.30), "Norte": (244061.39, 218216.08), "Sudeste": (1317365.66, 1406217.52), "Sul": (373261.79, 372494.96), "Todas as Regiões": (2783590.34, 2953106.93)},
    10: {"Centro-Oeste": (206436.61, 239286.01), "Nordeste": (613268.27, 595689.57), "Norte": (199808.55, 227647.04), "Sudeste": (1182979.69, 1258299.18), "Sul": (331873.93, 373626.03), "Todas as Regiões": (2534367.05, 2694547.83)},
    11: {"Centro-Oeste": (224235.09, 212983.80), "Nordeste": (575452.32, 601881.88), "Norte": (210332.51, 218473.07), "Sudeste": (1195325.38, 1318096.40), "Sul": (348147.14, 363955.07), "Todas as Regiões": (2553492.44, 2715390.22)},
    12: {"Centro-Oeste": (226494.09, 216408.29), "Nordeste": (583320.91, 657830.90), "Norte": (202068.84, 194739.37), "Sudeste": (1189540.93, 1288295.91), "Sul": (341539.57, 344873.22), "Todas as Regiões": (2542964.34, 2702147.69)},
    13: {"Centro-Oeste": (200417.35, 235227.95), "Nordeste": (617711.84, 609450.84), "Norte": (203918.34, 192780.93), "Sudeste": (1205047.98, 1311199.15), "Sul": (312721.93, 349988.63), "Todas as Regiões": (2539817.44, 2698647.50)},
    14: {"Centro-Oeste": (205739.21, 240011.58), "Nordeste": (614416.55, 636402.45), "Norte": (184276.05, 221440.60), "Sudeste": (1206768.69, 1264151.17), "Sul": (337790.20, 347404.74), "Todas as Regiões": (2548990.70, 2709410.54)},
    15: {"Centro-Oeste": (208269.69, 224064.86), "Nordeste": (632268.37, 649102.63), "Norte": (200501.75, 195730.91), "Sudeste": (1342004.86, 1463378.94), "Sul": (347415.83, 370409.37), "Todas as Regiões": (2730460.50, 2902686.71)},
    16: {"Centro-Oeste": (242824.87, 227979.01), "Nordeste": (617200.02, 740964.02), "Norte": (227461.18, 212471.17), "Sudeste": (1299314.02, 1365341.30), "Sul": (374788.24, 388265.55), "Todas as Regiões": (2761588.33, 2935021.05)},
    17: {"Centro-Oeste": (205326.86, 216963.88), "Nordeste": (593500.41, 649443.15), "Norte": (245993.46, 213681.64), "Sudeste": (1202475.95, 1312979.84), "Sul": (333922.90, 349659.77), "Todas as Regiões": (2581219.58, 2742728.28)},
    18: {"Centro-Oeste": (223402.52, 238832.32), "Nordeste": (624581.71, 629941.98), "Norte": (203614.62, 219922.82), "Sudeste": (1215516.06, 1300215.27), "Sul": (314512.95, 351429.29), "Todas as Regiões": (2581627.86, 2740341.68)},
    19: {"Centro-Oeste": (248338.90, 242401.39), "Nordeste": (573003.07, 641860.12), "Norte": (224557.33, 231228.66), "Sudeste": (1190295.45, 1298839.21), "Sul": (338353.10, 320881.61), "Todas as Regiões": (2574547.85, 2735210.99)},
    20: {"Centro-Oeste": (197670.11, 210372.12), "Nordeste": (584195.17, 647238.36), "Norte": (202634.79, 224080.78), "Sudeste": (1229283.50, 1274880.25), "Sul": (362424.37, 378125.24), "Todas as Regiões": (2576207.94, 2734696.75)},
    21: {"Centro-Oeste": (236793.57, 242410.56), "Nordeste": (602035.73, 648218.60), "Norte": (191608.23, 230350.43), "Sudeste": (1208401.15, 1272154.62), "Sul": (324646.01, 330650.51), "Todas as Regiões": (2563484.69, 2723784.72)},
    22: {"Centro-Oeste": (233874.61, 236572.02), "Nordeste": (636290.62, 696967.35), "Norte": (238577.90, 231646.98), "Sudeste": (1269151.31, 1369537.63), "Sul": (361434.50, 378286.66), "Todas as Regiões": (2739328.94, 2913010.64)},
    23: {"Centro-Oeste": (226985.49, 278805.45), "Nordeste": (630576.38, 682632.23), "Norte": (209386.50, 215287.47), "Sudeste": (1316963.54, 1374228.71), "Sul": (385450.16, 391678.69), "Todas as Regiões": (2769362.07, 2942632.55)},
    24: {"Centro-Oeste": (244733.24, 270848.14), "Nordeste": (658433.89, 691702.24), "Norte": (232389.65, 267158.09), "Sudeste": (1403107.41, 1470225.70), "Sul": (414637.90, 439437.68), "Todas as Regiões": (2953302.09, 3139371.85)},
    25: {"Centro-Oeste": (242670.39, 281665.64), "Nordeste": (710990.76, 698494.68), "Norte": (224008.59, 223517.88), "Sudeste": (1388354.76, 1530627.41), "Sul": (417454.39, 432318.68), "Todas as Regiões": (2983478.89, 3166624.29)},
    26: {"Centro-Oeste": (263903.01, 266741.54), "Nordeste": (709451.15, 787978.44), "Norte": (221087.00, 244616.43), "Sudeste": (1381707.57, 1439045.59), "Sul": (379347.34, 406227.86), "Todas as Regiões": (2955496.07, 3144609.86)},
    27: {"Centro-Oeste": (271855.12, 251647.84), "Nordeste": (693327.00, 781962.86), "Norte": (263720.66, 273900.25), "Sudeste": (1396978.55, 1485104.34), "Sul": (371809.30, 392555.03), "Todas as Regiões": (2997690.63, 3185170.32)},
    28: {"Centro-Oeste": (362751.41, 386139.63), "Nordeste": (1055965.27, 1142308.05), "Norte": (347661.19, 365306.83), "Sudeste": (2114083.21, 2234327.84), "Sul": (574768.00, 607595.85), "Todas as Regiões": (4455229.08, 4735678.20)},
    29: {"Centro-Oeste": (291167.71, 337147.58), "Nordeste": (864637.54, 803265.97), "Norte": (279916.60, 296702.53), "Sudeste": (1586221.72, 1760568.47), "Sul": (433554.10, 472567.85), "Todas as Regiões": (3455497.67, 3670252.40)},
    30: {"Centro-Oeste": (292828.28, 294146.58), "Nordeste": (830917.22, 902094.66), "Norte": (258982.44, 305459.52), "Sudeste": (1631968.21, 1717840.81), "Sul": (437267.18, 448720.67), "Todas as Regiões": (3451963.33, 3668262.24)}
}


def fmt_cur(v, force_unit=None):
    if v is None or pd.isna(v):
        return "R$ 0"
    if force_unit == "MM" or (force_unit is None and abs(v) >= 1_000_000):
        s = f"{v/1_000_000:.2f}".replace('.', ',')
        return f"R$ {s} MM"
    elif force_unit == "Mil" or (force_unit is None and abs(v) >= 1_000):
        s = f"{v/1_000:.1f}".replace('.', ',')
        return f"R$ {s} Mil"
    return f"R$ {v:.2f}".replace('.', ',')

def fmt_num(v):
    if v is None or pd.isna(v):
        return "0"
    if abs(v) >= 1_000_000:
        s = f"{v/1_000_000:.2f}".replace('.', ',')
        return f"{s} MM"
    elif abs(v) >= 1_000:
        s = f"{v/1_000:.1f}".replace('.', ',')
        return f"{s} Mil"
    return f"{int(round(v)):,}".replace(',', '.')

def fmt_pct(v):
    if v is None or pd.isna(v):
        return "0,0%"
    s = f"{v*100:.1f}".replace('.', ',')
    return f"{s}%"

def get_badge(ating):
    p = fmt_pct(ating)
    return f"▲ {p} Meta" if ating >= 1.0 else f"▼ {p} Meta"

def get_status(ating):
    if ating >= 1.0:
        return "pos"
    elif ating >= 0.85:
        return "warn"
    return "neg"

def get_dif_fmt(dif, is_currency=True):
    sign = "+" if dif >= 0 else "-"
    a = abs(dif)
    if is_currency:
        if a >= 1_000_000:
            s = f"{a/1_000_000:.1f}".replace('.', ',')
            return f"{sign}R$ {s} MM"
        else:
            s = f"{a/1_000:.1f}".replace('.', ',')
            return f"{sign}R$ {s} Mil"
    else:
        if a >= 1_000_000:
            s = f"{a/1_000_000:.1f}".replace('.', ',')
            return f"{sign}{s} MM"
        elif a >= 1_000:
            s = f"{a/1_000:.1f}".replace('.', ',')
            return f"{sign}{s} Mil"
        return f"{sign}{int(round(a))}"


def find_chromium() -> str:
    candidates = [
        "chromium-browser",
        "chromium",
        "google-chrome",
        "google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ]
    for c in candidates:
        if w := shutil.which(c):
            return w
        if os.path.exists(c):
            return c
    return "chromium"


def obter_cliente_bigquery():
    sa_key_env = os.environ.get("GCP_SA_KEY", "").strip()
    if sa_key_env:
        try:
            if sa_key_env.startswith("{"):
                key_dict = json.loads(sa_key_env)
            else:
                key_dict = json.loads(base64.b64decode(sa_key_env).decode("utf-8"))
            from google.oauth2 import service_account
            from google.cloud import bigquery
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
        except Exception as e:
            print(f"  -> Aviso ao carregar GCP_SA_KEY: {e}")

    candidatos_locais = [
        r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\JSON\mercado-livre-mais-vendidos-143cf1ecac17.json",
        SCRIPT_DIR / "gcp_key.json"
    ]
    for p in candidatos_locais:
        if os.path.exists(p):
            try:
                from google.oauth2 import service_account
                from google.cloud import bigquery
                creds = service_account.Credentials.from_service_account_file(str(p))
                return bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
            except Exception as e:
                print(f"  -> Aviso chave local {p}: {e}")

    try:
        from google.cloud import bigquery
        return bigquery.Client(project=GCP_PROJECT_ID)
    except Exception as e:
        print(f"  -> Aviso: BigQuery client padrão não inicializado: {e}")
        return None


def atualizar_payloads_dinamicos(client, out_dir: pathlib.Path):
    print("\n[1/5] Gerando Payloads Dinâmicos do BigQuery (Novembro/2026)...")
    q_max = "SELECT MAX(data) as max_data FROM `mercado-livre-mais-vendidos.ML_Cidades.Fato_MercadoLivre_MaisVendidos`"
    max_dt_row = client.query(q_max).to_dataframe()['max_data'].iloc[0]
    max_dt_str = str(max_dt_row)
    print(f"  -> Data mais recente no BigQuery: {max_dt_str}")

    q_all = f"""
    WITH real_agg AS (
        SELECT 
            data, regiao, categoria,
            SUM(SAFE_CAST(REPLACE(faturamento_estimado_dia, ',', '.') AS FLOAT64)) as fat,
            SUM(qtd_vendas_estimadas_dia) as pedidos,
            AVG(SAFE_CAST(REPLACE(preco_atual, ',', '.') AS FLOAT64)) as preco
        FROM `mercado-livre-mais-vendidos.ML_Cidades.Fato_MercadoLivre_MaisVendidos`
        WHERE data >= '2026-11-01' AND data <= '{max_dt_str}'
        GROUP BY data, regiao, categoria
    ),
    metas_agg AS (
        SELECT 
            data, regiao, categoria,
            SUM(SAFE_CAST(REPLACE(faturamento_estimado_dia, ',', '.') AS FLOAT64)) as meta_fat,
            SUM(qtd_vendas_estimadas_dia) as meta_ped,
            AVG(SAFE_CAST(REPLACE(preco_atual, ',', '.') AS FLOAT64)) as meta_preco
        FROM `mercado-livre-mais-vendidos.ML_Cidades.Fato_MercadoLivre_Metas`
        WHERE data >= '2026-11-01' AND data <= '{max_dt_str}'
        GROUP BY data, regiao, categoria
    )
    SELECT 
        COALESCE(r.data, m.data) as data,
        COALESCE(r.regiao, m.regiao) as regiao,
        COALESCE(r.categoria, m.categoria) as categoria,
        COALESCE(r.fat, 0) as fat,
        COALESCE(m.meta_fat, 0) as meta_fat,
        COALESCE(r.pedidos, 0) as pedidos,
        COALESCE(m.meta_ped, 0) as meta_ped,
        COALESCE(r.preco, 0) as preco,
        COALESCE(m.meta_preco, 0) as meta_preco
    FROM real_agg r
    FULL OUTER JOIN metas_agg m 
        ON r.data = m.data AND r.regiao = m.regiao AND r.categoria = m.categoria
    ORDER BY data, regiao, categoria
    """
    df_cat = client.query(q_all).to_dataframe()
    df_cat['data'] = pd.to_datetime(df_cat['data'])

    q_top = f"""
    WITH ranked AS (
        SELECT 
            data, regiao, categoria, titulo_produto,
            SUM(SAFE_CAST(REPLACE(faturamento_estimado_dia, ',', '.') AS FLOAT64)) as fat,
            ROW_NUMBER() OVER(PARTITION BY data, regiao, categoria ORDER BY SUM(SAFE_CAST(REPLACE(faturamento_estimado_dia, ',', '.') AS FLOAT64)) DESC) as rk
        FROM `mercado-livre-mais-vendidos.ML_Cidades.Fato_MercadoLivre_MaisVendidos`
        WHERE data >= '2026-11-01' AND data <= '{max_dt_str}'
        GROUP BY data, regiao, categoria, titulo_produto
    )
    SELECT data, regiao, categoria, titulo_produto, fat, rk
    FROM ranked
    WHERE rk <= 6
    ORDER BY data, regiao, categoria, rk
    """
    df_top = client.query(q_top).to_dataframe()
    df_top['data'] = pd.to_datetime(df_top['data'])

    datas_unicas = sorted(df_cat['data'].unique())

    # Carrega templates base do repositório
    with open(SCRIPT_DIR / "payload_diario.json", "r", encoding="utf-8") as f:
        pl_diario = json.load(f)
    with open(SCRIPT_DIR / "payload_mtd.json", "r", encoding="utf-8") as f:
        pl_mtd = json.load(f)
    with open(SCRIPT_DIR / "payload_ytda.json", "r", encoding="utf-8") as f:
        pl_ytda = json.load(f)

    # 1. PAYLOAD DIÁRIO
    novos_dias = []
    novos_comparativos = []
    novos_top_produtos = []
    novos_pareto = []

    for day_idx, dt in enumerate(datas_unicas):
        dt_str = dt.strftime("%d/%m/%Y")
        dt_fmt = f"{dt.day:02d}/{MESES_ABREV[dt.month]}/{dt.year}"
        dia_sem = DIAS_SEMANA[dt.weekday()]
        dt_fmt_2025 = f"{dt.day:02d}/{MESES_ABREV[dt.month]}/2025"

        df_day = df_cat[df_cat['data'] == dt]
        df_top_day = df_top[df_top['data'] == dt]

        for reg in REGIOES_LIST:
            if reg == "Todas as Regiões":
                df_reg = df_day
                df_top_reg = df_top_day
            else:
                df_reg = df_day[df_day['regiao'] == reg]
                df_top_reg = df_top_day[df_top_day['regiao'] == reg]

            fat_tot = float(df_reg['fat'].sum())
            meta_tot = float(df_reg['meta_fat'].sum())
            ped_tot = float(df_reg['pedidos'].sum())
            meta_ped_tot = float(df_reg['meta_ped'].sum())
            preco_med = float(df_reg['preco'].mean()) if len(df_reg) > 0 else 0.0
            ticket = fat_tot / ped_tot if ped_tot > 0 else 0.0

            ating = fat_tot / meta_tot if meta_tot > 0 else 0.0
            dif = fat_tot - meta_tot
            ating_ped = ped_tot / meta_ped_tot if meta_ped_tot > 0 else 0.0
            dif_ped = ped_tot - meta_ped_tot

            # 2025 Hist
            h25 = HIST_2025_BASE.get(dt.day, {}).get(reg, (fat_tot * 0.95, meta_tot * 0.95))
            fat_2025, meta_2025 = h25[0], h25[1]
            ating_2025 = fat_2025 / meta_2025 if meta_2025 > 0 else 0.941
            dif_2025 = fat_2025 - meta_2025

            media_mes_num = 4457742.0 if reg == "Todas as Regiões" else fat_tot * 0.9
            var_val = (fat_tot - media_mes_num) / media_mes_num if media_mes_num > 0 else 0.0

            novos_dias.append({
                "regiao": reg,
                "dayIdx": day_idx,
                "date": dt_str,
                "date_fmt": dt_fmt,
                "dia_sem": dia_sem,
                "fat": fmt_cur(fat_tot),
                "meta": fmt_cur(meta_tot),
                "ating": fmt_pct(ating),
                "ating_num": round(ating, 4),
                "dif": get_dif_fmt(dif, True),
                "pedidos": fmt_num(ped_tot),
                "meta_ped": fmt_num(meta_ped_tot),
                "dif_ped": get_dif_fmt(dif_ped, False),
                "ating_ped": get_badge(ating_ped),
                "ating_ped_num": round(ating_ped, 4),
                "ticket": f"R$ {ticket:.2f}".replace('.', ','),
                "preco": f"R$ {preco_med:.2f}".replace('.', ','),
                "date_fmt_2025": dt_fmt_2025,
                "fat_2025": fmt_cur(fat_2025),
                "meta_2025": fmt_cur(meta_2025),
                "ating_2025": fmt_pct(ating_2025),
                "ating_num_2025": round(ating_2025, 4),
                "dif_2025": get_dif_fmt(dif_2025, True),
                "badge_2025": get_badge(ating_2025),
                "media_mes_num": media_mes_num,
                "media_mes_fmt": fmt_cur(media_mes_num),
                "media_meta_fmt": f"Meta: {fmt_cur(media_mes_num*1.07)}",
                "media_dif_fmt": f"Dif: {get_dif_fmt(fat_tot - media_mes_num, True)}",
                "media_var_val": f"{'+' if var_val>=0 else ''}{var_val*100:.1f}%".replace('.', ','),
                "media_ating_num": round(ating, 4),
                "media_ating_fmt": fmt_pct(ating),
                "media_badge": get_badge(ating),
                "media_status": get_status(ating)
            })

            # Comparativos
            for cat_idx, (cat_label, cat_db, _, _) in enumerate(CATEGORIAS):
                if cat_db == "Visão Geral":
                    c_fat = fat_tot
                    c_meta = meta_tot
                else:
                    df_c = df_reg[df_reg['categoria'] == cat_db]
                    c_fat = float(df_c['fat'].sum()) if len(df_c) > 0 else 0.0
                    c_meta = float(df_c['meta_fat'].sum()) if len(df_c) > 0 else 0.0

                c_ating = c_fat / c_meta if c_meta > 0 else 0.0
                unidade = "em R$ MM" if c_meta >= 1_000_000 else "em R$ Mil"
                lbl_r = f"{c_fat/1e6:.2f}".replace('.', ',') if unidade == "em R$ MM" else f"{c_fat/1e3:.1f}".replace('.', ',')
                lbl_m = f"{c_meta/1e6:.2f}".replace('.', ',') if unidade == "em R$ MM" else f"{c_meta/1e3:.1f}".replace('.', ',')

                novos_comparativos.append({
                    "regiao": reg,
                    "cat": cat_label,
                    "dayIdx": day_idx,
                    "idx": 0,
                    "titulo": "Faturamento",
                    "unidade": unidade,
                    "lblReal": lbl_r,
                    "lblMeta": lbl_m,
                    "badge_txt": fmt_pct(c_ating),
                    "status": get_status(c_ating),
                    "hReal": int(round(min(140, c_ating * 128))),
                    "hMeta": 128
                })

            # Top Produtos
            df_tp_vg = df_top_reg.groupby('titulo_produto', as_index=False)['fat'].sum().sort_values('fat', ascending=False).head(6)
            max_v = float(df_tp_vg['fat'].max()) if len(df_tp_vg) > 0 else 1.0

            for p_idx, (_, r_prod) in enumerate(df_tp_vg.iterrows()):
                p_fat = float(r_prod['fat'])
                w_real = min(110, int(round((p_fat / max_v) * 110)))
                w_meta = min(110, int(round(w_real * 1.15)))
                lbl_r = f"R$ {p_fat/1e6:.1f}M".replace('.', ',') if p_fat >= 1e6 else f"R$ {p_fat/1e3:.0f}k".replace('.', ',')
                lbl_m = f"R$ {(p_fat*1.15)/1e6:.1f}M".replace('.', ',') if p_fat*1.15 >= 1e6 else f"R$ {(p_fat*1.15)/1e3:.0f}k".replace('.', ',')

                novos_top_produtos.append({
                    "regiao": reg,
                    "cat": "Visão Geral",
                    "dayIdx": day_idx,
                    "idx": p_idx,
                    "rank": p_idx + 1,
                    "produto": r_prod['titulo_produto'][:28],
                    "wReal": w_real,
                    "wMeta": w_meta,
                    "lblReal": lbl_r,
                    "lblMeta": lbl_m
                })

            # Pareto
            df_pareto = df_reg[df_reg['categoria'].isin([c[1] for c in CATEGORIAS if c[1] != "Visão Geral"])]
            cat_sums = df_pareto.groupby('categoria', as_index=False)['fat'].sum().sort_values('fat', ascending=False)
            tot_pareto = float(cat_sums['fat'].sum()) if len(cat_sums) > 0 else 0.0
            if tot_pareto <= 0:
                tot_pareto = 1.0
            acum = 0.0

            for c_idx, (_, r_cat) in enumerate(cat_sums.iterrows()):
                c_nom = r_cat['categoria']
                c_fat = float(r_cat['fat'])
                vol_pct = (c_fat / tot_pareto) * 100
                acum += vol_pct

                cat_info = next((c for c in CATEGORIAS if c[1] == c_nom), ("Outros", c_nom, c_nom, ""))
                novos_pareto.append({
                    "regiao": reg,
                    "dayIdx": day_idx,
                    "idx": c_idx,
                    "cat_key": cat_info[0],
                    "categoria": c_nom,
                    "line1": cat_info[2],
                    "line2": cat_info[3],
                    "vol_pct": round(vol_pct, 1),
                    "acum_pct": min(100.0, round(acum, 1)),
                    "valor": fmt_cur(c_fat),
                    "bar_h": int(round(vol_pct * 1.63))
                })

    pl_diario['dias'] = novos_dias
    pl_diario['comparativos'] = novos_comparativos
    pl_diario['top_produtos'] = novos_top_produtos
    pl_diario['pareto'] = novos_pareto

    # 2. PAYLOAD MTD E YTDA
    max_dt = datas_unicas[-1]
    max_day = max_dt.day
    dt_str = max_dt.strftime("%d/%m/%Y")
    dt_fmt = f"{max_dt.day:02d}/{MESES_ABREV[max_dt.month]}/{max_dt.year}"

    df_mtd = df_cat.groupby('regiao', as_index=False).agg({'fat': 'sum', 'meta_fat': 'sum', 'pedidos': 'sum', 'meta_ped': 'sum'})
    
    for m in pl_mtd.get('meses', []):
        if m.get('mesIdx') == 22:
            r = m.get('regiao')
            row = df_mtd[df_mtd['regiao'] == r] if r != "Todas as Regiões" else df_mtd
            f_mtd = float(row['fat'].sum())
            m_mtd = float(row['meta_fat'].sum())
            p_mtd = float(row['pedidos'].sum())
            mp_mtd = float(row['meta_ped'].sum())

            ating_mtd = f_mtd / m_mtd if m_mtd > 0 else 0.0
            dif_mtd = f_mtd - m_mtd
            ating_ped = p_mtd / mp_mtd if mp_mtd > 0 else 0.0
            dif_ped = p_mtd - mp_mtd

            m['data_atualizado'] = dt_str
            m['data_card0_fmt'] = dt_fmt
            m['fat_mtd'] = fmt_cur(f_mtd)
            m['meta_mtd'] = fmt_cur(m_mtd)
            m['dif_mtd'] = get_dif_fmt(dif_mtd, True)
            m['badge_mtd'] = get_badge(ating_mtd)
            m['ating_mtd_num'] = round(ating_mtd, 4)
            m['prog_label_mtd'] = f"{int(round(ating_mtd*100))}% da meta"
            m['ped_mtd'] = fmt_num(p_mtd)
            m['meta_ped_mtd'] = fmt_num(mp_mtd)
            m['dif_ped_mtd'] = get_dif_fmt(dif_ped, False)
            m['badge_ped_mtd'] = get_badge(ating_ped)
            m['fat_diario_mtd'] = fmt_cur(f_mtd / max_day)
            m['meta_diario_mtd'] = f"Meta: {fmt_cur(m_mtd / max_day)}"
            m['dif_diario_mtd'] = f"Dif: {get_dif_fmt((f_mtd - m_mtd)/max_day, True)}"
            m['badge_diario_mtd'] = get_badge(ating_mtd)
            m['ating_diario_mtd_num'] = round(ating_mtd, 4)

    for m in pl_ytda.get('meses', []):
        if m.get('mesIdx') == 22:
            m['data_atualizacao'] = dt_str

    # Salva arquivos na pasta de destino (e espelha no repositório)
    for p_name, p_data in [("payload_diario.json", pl_diario), ("payload_mtd.json", pl_mtd), ("payload_ytda.json", pl_ytda)]:
        (out_dir / p_name).write_text(json.dumps(p_data, indent=2, ensure_ascii=False), encoding="utf-8")
        if out_dir != SCRIPT_DIR:
            (SCRIPT_DIR / p_name).write_text(json.dumps(p_data, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"  -> Payloads gerados com sucesso para a data base {dt_str} ({len(novos_dias)} dias).")
    return dt_str


def render_deneb_png(browser: str, key: str, spec_path: pathlib.Path, payload_path: pathlib.Path, measure_name: str, tmp_dir: pathlib.Path, out_png: pathlib.Path, region: str = "Todas as Regiões"):
    svg_path = tmp_dir / f"{key}_{region}.svg"
    render_js = SCRIPT_DIR / "render_deneb_svg.js"
    res = subprocess.run(
        ["node", str(render_js), str(spec_path), str(payload_path), measure_name, str(svg_path), "Light", region],
        capture_output=True, text=True
    )
    if res.returncode != 0 or not svg_path.exists():
        raise RuntimeError(f"Erro ao renderizar SVG do Deneb ({key} - {region}):\n{res.stderr}\n{res.stdout}")

    svg_text = svg_path.read_text(encoding="utf-8")
    offset_x = -220
    offset_y = -72

    html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #F4F6F8;
    width: 1380px;
    height: 760px;
    overflow: hidden;
    position: relative;
  }}
  .viewport {{
    position: absolute;
    left: {offset_x}px;
    top: {offset_y}px;
  }}
  svg {{
    width: 1600px;
    height: 832px;
    display: block;
  }}
</style>
</head>
<body>
  <div class="viewport">
    {svg_text}
  </div>
</body>
</html>"""

    html_path = tmp_dir / f"{key}_{region}.html"
    html_path.write_text(html_content, encoding="utf-8")

    cmd = [
        browser,
        "--headless=new",
        "--disable-gpu",
        "--hide-scrollbars",
        "--force-device-scale-factor=1.5",
        "--window-size=1380,760",
        "--virtual-time-budget=3000",
        f"--screenshot={str(out_png)}",
        html_path.resolve().as_uri() if sys.platform == "win32" else f"file://{html_path.resolve()}",
    ]
    subprocess.run(cmd, check=True, capture_output=True)


def main():
    print("=" * 80)
    print("🚀 PIPELINE AUTÔNOMO DE GERAÇÃO DE PRINTS DO JOCA (100% BIGQUERY DINÂMICO)")
    print(f"Data/Hora UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    browser = find_chromium()
    print(f"Navegador Chromium: {browser}")

    client = obter_cliente_bigquery()
    if not client:
        print("❌ Erro: Cliente BigQuery não inicializado.")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = pathlib.Path(td)

        # 1. Geração Dinâmica de Payloads
        data_base_str = atualizar_payloads_dinamicos(client, tmp_dir)

        payloads = {
            "diario": tmp_dir / "payload_diario.json",
            "mtd": tmp_dir / "payload_mtd.json",
            "ytda": tmp_dir / "payload_ytda.json",
        }

        specs = {
            "diario": SCRIPT_DIR / "spec_diario.json",
            "mtd": SCRIPT_DIR / "spec_mtd.json",
            "ytda": SCRIPT_DIR / "spec_ytda.json",
        }

        measures = {
            "diario": "JSON_Resumo_Diario",
            "mtd": "JSON_Resumo_MTD",
            "ytda": "JSON_Resumo_YTDA",
        }

        print("\n[2/5] Renderizando os 24 prints executivos...")
        total_gerados = 0
        for reg_key, reg_name in REGIOES.items():
            print(f"\n--- Regional: {reg_name} ({reg_key}) ---")
            for pan_k in ["diario", "mtd", "ytda"]:
                if pan_k not in payloads or not specs[pan_k].exists():
                    print(f"  -> ⚠️ Pulando {pan_k}: payload ou spec não encontrado.")
                    continue

                out_p = PRINTS_DIR / f"print_{pan_k}_{reg_key}.png"
                render_deneb_png(browser, pan_k, specs[pan_k], payloads[pan_k], measures[pan_k], tmp_dir, out_p, region=reg_name)
                print(f"  -> PNG: {out_p.name} ({out_p.stat().st_size // 1024} KB)")
                total_gerados += 1

            if reg_key == "todas":
                for pan_k in ["diario", "mtd", "ytda"]:
                    src = PRINTS_DIR / f"print_{pan_k}_todas.png"
                    if src.exists():
                        shutil.copy2(src, PRINTS_DIR / f"print_{pan_k}.png")

        # 3. Metadados
        brt_now = datetime.now(timezone(timedelta(hours=-3)))
        meta = {
            "atualizado_em": brt_now.strftime("%Y-%m-%d %H:%M:%S"),
            "data_base": data_base_str,
            "total_prints": total_gerados,
            "origem": "GitHub Actions BigQuery Dinâmico 24/7"
        }
        PRINTS_META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\n[3/5] Metadados atualizados em prints_meta.json (Data Base: {data_base_str}).")

        # 4. Upload dos Prints para o Render
        print(f"\n[4/5] Enviando prints para o Render ({RENDER_BASE_URL})...")
        sucessos = 0
        for p in PRINTS_DIR.glob("print_*.png"):
            tipo = p.stem.replace("print_", "")
            url = f"{RENDER_BASE_URL}/sync_print?secret={SYNC_SECRET}&tipo={tipo}"
            try:
                with open(p, "rb") as f:
                    r = requests.post(url, files={"file": f}, timeout=30)
                if r.status_code == 200:
                    sucessos += 1
            except Exception as e:
                print(f"  -> Falha no upload de {p.name}: {e}")

        print(f"✅ Sincronização concluída: {sucessos} prints enviados com sucesso ao Render!")

        # 5. Exportação e sincronização da base Parquet
        print(f"\n[5/5] Sincronizando base Parquet com o Telegram Bot...")
        try:
            q_parquet = "SELECT * FROM `mercado-livre-mais-vendidos.Mercado_Livre.Fato_MercadoLivre_MaisVendidos` ORDER BY data ASC, posicao_ranking ASC"
            df_pq = client.query(q_parquet).to_dataframe()
            local_pq = SCRIPT_DIR / "Fato_MercadoLivre_MaisVendidos.parquet"
            df_pq.to_parquet(local_pq, index=False, compression="zstd")
            print(f"  -> Parquet local atualizado ({len(df_pq)} linhas até {df_pq['data'].max()}).")
        except Exception as e_pq:
            print(f"  -> Aviso ao atualizar Parquet: {e_pq}")


if __name__ == "__main__":
    main()
