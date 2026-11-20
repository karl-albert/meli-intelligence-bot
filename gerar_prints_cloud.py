# -*- coding: utf-8 -*-
"""
========================================================================================
GERADOR E SINCRONIZADOR DE PRINTS EXECUTIVOS 100% EM NUVEM (GITHUB ACTIONS)
========================================================================================
Executa no Ubuntu (GitHub Actions) de forma totalmente autonoma e independente do notebook.
  4 Paineis x 6 Recortes Regionais = 24 Prints:
    • diario     (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • mtd        (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • ytda       (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
    • detalhado  (Todas as Regioes, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)

Origem dos Dados:
  1. REST API Oficial do Microsoft Fabric / Power BI (se Azure Secrets configuradas)
  2. Fallback para Payloads JSON locais cacheados no repositorio.
========================================================================================
"""

import os
import sys
import json
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
SYNC_SECRET = os.environ.get("SYNC_SECRET", "meli_joca_sync_2026_karl")

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
    ]
    for c in candidates:
        if shutil_which := subprocess.run(["which", c], capture_output=True, text=True):
            if shutil_which.returncode == 0:
                return c
        if os.path.exists(c):
            return c
    return "chromium"


def obter_payloads_fabric(tmp_dir: pathlib.Path) -> dict:
    """Tenta consultar a REST API do Microsoft Fabric / Power BI para extrair DAX ao vivo."""
    tenant_id = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    workspace_id = os.environ.get("WORKSPACE_ID", "0c2a1a5e-4519-4e4e-b3e6-14288c11291d")
    dataset_id = os.environ.get("DATASET_ID_MELI", "092ce1ab-96e2-4d9e-bec8-938c597015dc")

    payloads = {}
    if client_id and client_secret and tenant_id and dataset_id:
        print("[1/4] Consultando Microsoft Fabric via Power BI REST API...")
        token_url = f"https://login.microsoftonline.com/{tenant_id.strip()}/oauth2/v2.0/token"
        token_data = {
            "client_id": client_id.strip(),
            "client_secret": client_secret.strip(),
            "grant_type": "client_credentials",
            "scope": "https://analysis.windows.net/powerbi/api/.default"
        }
        try:
            r_tok = requests.post(token_url, data=token_data, timeout=15)
            if r_tok.status_code == 200:
                token = r_tok.json().get("access_token")
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
                queries = {
                    "diario": "EVALUATE ROW('v', [JSON_Resumo_Diario])",
                    "mtd": "EVALUATE ROW('v', [JSON_Resumo_MTD])",
                    "ytda": "EVALUATE ROW('v', [JSON_Resumo_YTDA])"
                }
                for k, q in queries.items():
                    r_q = requests.post(api_url, headers=headers, json={"queries": [{"query": q}]}, timeout=30)
                    if r_q.status_code == 200:
                        rows = r_q.json().get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                        if rows:
                            val = list(rows[0].values())[0]
                            out_p = tmp_dir / f"payload_{k}.json"
                            out_p.write_text(val, encoding="utf-8")
                            payloads[k] = out_p
                            # Salva cache
                            (SCRIPT_DIR / f"payload_{k}.json").write_text(val, encoding="utf-8")
                            print(f"  -> Payload '{k}' extraido do Fabric ({len(val):,} bytes)")
            else:
                print(f"  -> Aviso Azure AD: {r_tok.status_code} - {r_tok.text}")
        except Exception as e:
            print(f"  -> Aviso ao conectar na REST API do Fabric: {e}")

    # Fallback para payloads cacheados
    for k in ["diario", "mtd", "ytda"]:
        if k not in payloads:
            cache_file = SCRIPT_DIR / f"payload_{k}.json"
            if cache_file.exists():
                out_p = tmp_dir / f"payload_{k}.json"
                out_p.write_text(cache_file.read_text(encoding="utf-8"), encoding="utf-8")
                payloads[k] = out_p
                print(f"  -> Payload '{k}' carregado do cache local ({cache_file.stat().st_size:,} bytes)")

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
    print("🚀 PIPELINE AUTÔNOMO DE GERAÇÃO DE PRINTS DO JOCA (100% NUVEM)")
    print(f"Data/Hora UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    browser = find_chromium()
    print(f"Navegador Chromium: {browser}")

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = pathlib.Path(td)
        payloads = obter_payloads_fabric(tmp_dir)
        if not payloads:
            print("❌ Erro: Nenhum payload disponivel para renderizacao.")
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
        for reg_key, reg_name in REGIOES.items():
            print(f"\n--- Regional: {reg_name} ({reg_key}) ---")
            for pan_k in ["diario", "mtd", "ytda"]:
                out_p = PRINTS_DIR / f"print_{pan_k}_{reg_key}.png"
                render_deneb_png(browser, pan_k, specs[pan_k], payloads[pan_k], measures[pan_k], tmp_dir, out_p, region=reg_name)
                print(f"  -> PNG: {out_p.name} ({out_p.stat().st_size // 1024} KB)")

            # Retrocompatibilidade Brasil
            if reg_key == "todas":
                for pan_k in ["diario", "mtd", "ytda"]:
                    shutil.copy2(PRINTS_DIR / f"print_{pan_k}_todas.png", PRINTS_DIR / f"print_{pan_k}.png")

        # Atualiza metadados
        brt_now = datetime.now(timezone(timedelta(hours=-3)))
        meta = {
            "atualizado_em": brt_now.strftime("%Y-%m-%d %H:%M:%S"),
            "data_base": brt_now.strftime("%d/%m/%Y"),
            "total_prints": 24,
            "origem": "GitHub Actions Cloud Automático"
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

        print(f"✅ Sincronizacao concluida: {sucessos} prints enviados com sucesso ao Render!")

if __name__ == "__main__":
    main()
