# -*- coding: utf-8 -*-
"""
========================================================================================
GERADOR E SINCRONIZADOR DE PRINTS EXECUTIVOS 100% EM NUVEM (BIGQUERY + GITHUB ACTIONS)
========================================================================================
Executa no Ubuntu (GitHub Actions) de forma totalmente autonoma e integrada ao BigQuery.
  4 Paineis x 6 Recortes Regionais = 24 Prints:
    • diario     (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • mtd        (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • ytda       (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • detalhado  (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)

Origem dos Dados:
  1. Google BigQuery (mercado-livre-mais-vendidos.ML_Cidades)
  2. Payloads JSON sincronizados (payload_diario.json, payload_mtd.json, payload_ytda.json)
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
from datetime import datetime, timezone, timedelta
from PIL import Image

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
    """Conecta ao Google BigQuery via Secret do GitHub (GCP_SA_KEY) ou chave local."""
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

    # Fallback local para desenvolvimento/testes
    candidatos_locais = [
        r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\JSON\mercado-livre-mais-vendidos-143cf1ecac17.json",
        pathlib.Path(__file__).parent / "gcp_key.json"
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


def validar_dados_bigquery():
    """Valida a atualização da base Fato no BigQuery."""
    print("[1/4] Consultando Google BigQuery (mercado-livre-mais-vendidos.ML_Cidades)...")
    try:
        client = obter_cliente_bigquery()
        if not client:
            print("  -> Aviso: BigQuery não disponível diretamente neste ambiente. Prosseguindo com payloads cacheados.")
            return None

        query = "SELECT MAX(data) as max_data, COUNT(*) as total FROM `mercado-livre-mais-vendidos.ML_Cidades.Fato_MercadoLivre_MaisVendidos`"
        df = client.query(query).to_dataframe()
        max_dt = df["max_data"].iloc[0]
        total = df["total"].iloc[0]
        print(f"  -> BigQuery conectado com sucesso! Data máxima: {max_dt} ({total:,} registros)")
        return str(max_dt)
    except Exception as e:
        print(f"  -> Aviso na consulta BigQuery: {e}. Prosseguindo com payloads sincronizados.")
        return None


def obter_payloads(tmp_dir: pathlib.Path) -> dict:
    """Carrega os payloads JSON (diario, mtd, ytda) do repositório."""
    payloads = {}
    for k in ["diario", "mtd", "ytda"]:
        cache_file = SCRIPT_DIR / f"payload_{k}.json"
        if cache_file.exists():
            out_p = tmp_dir / f"payload_{k}.json"
            out_p.write_text(cache_file.read_text(encoding="utf-8"), encoding="utf-8")
            payloads[k] = out_p
            print(f"  -> Payload '{k}' carregado ({cache_file.stat().st_size:,} bytes)")
        else:
            print(f"  -> ⚠️ AVISO: Payload '{k}' não encontrado em {cache_file.name}")

    return payloads


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
    print("🚀 PIPELINE AUTÔNOMO DE GERAÇÃO DE PRINTS DO JOCA (100% BIGQUERY)")
    print(f"Data/Hora UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    browser = find_chromium()
    print(f"Navegador Chromium: {browser}")

    # 1. Validação BigQuery
    data_bq = validar_dados_bigquery()

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = pathlib.Path(td)
        payloads = obter_payloads(tmp_dir)
        if not payloads:
            print("❌ Erro: Nenhum payload disponível para renderização.")
            sys.exit(1)

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

        print("\n[2/4] Renderizando os 24 prints executivos...")
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

            # Retrocompatibilidade Brasil (print_diario.png, print_mtd.png, print_ytda.png)
            if reg_key == "todas":
                for pan_k in ["diario", "mtd", "ytda"]:
                    src = PRINTS_DIR / f"print_{pan_k}_todas.png"
                    if src.exists():
                        shutil.copy2(src, PRINTS_DIR / f"print_{pan_k}.png")

        # Atualiza metadados
        brt_now = datetime.now(timezone(timedelta(hours=-3)))
        data_base_str = datetime.strptime(data_bq, "%Y-%m-%d").strftime("%d/%m/%Y") if data_bq else brt_now.strftime("%d/%m/%Y")

        meta = {
            "atualizado_em": brt_now.strftime("%Y-%m-%d %H:%M:%S"),
            "data_base": data_base_str,
            "total_prints": total_gerados,
            "origem": "GitHub Actions BigQuery Automático"
        }
        PRINTS_META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        print("\n[3/4] Metadados atualizados em prints_meta.json.")

        # Upload para o Render
        print(f"\n[4/4] Enviando prints para o Render ({RENDER_BASE_URL})...")
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


if __name__ == "__main__":
    main()
