# scripts/scraper_alter_material_venda.py
# -*- coding: utf-8 -*-

import os
import re
import csv
import time
import sys
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.alter.com.br/portal-do-parceiro/material-de-venda/"
OUT_DIR = "data/affix/raw"
CSV_PATH = "data/affix/alter_pdfs_manifest.csv"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)

def sanitize_name(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|#]+", "-", name).strip()
    return name or "arquivo.pdf"

def to_abs(url: str, base: str = BASE_URL) -> str:
    return url if bool(urlparse(url).netloc) else urljoin(base, url)

def write_csv(rows):
    ensure_dirs()
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "url"])
        for name, url in rows:
            w.writerow([name, url])

def download_file(url: str, path: str, timeout=60):
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(1 << 14):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"⚠️  Falha ao baixar {url}: {e}")
        return False

# ------------------------------------------------------------
# Passo 1: tentar só com requests/BS4
# ------------------------------------------------------------
def scrape_with_requests():
    print("🔎 [requests] Acessando página…")
    r = requests.get(BASE_URL, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "lxml")

    pdfs = set()

    # 1) <a href="...pdf">
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            absu = to_abs(href)
            # tenta pegar nome pelo texto do link
            text = (a.get_text(strip=True) or os.path.basename(urlparse(absu).path))
            name = sanitize_name(text)
            if not name.lower().endswith(".pdf"):
                # se não veio com .pdf no texto, force nome do arquivo
                name = os.path.basename(urlparse(absu).path) or (name + ".pdf")
            pdfs.add((name, absu))

    # 2) regex global no HTML (caso os links estejam em data-attrs / JS inline)
    for m in re.findall(r'(?i)href=["\']([^"\']+\.pdf[^"\']*)["\']', html):
        absu = to_abs(m)
        name = os.path.basename(urlparse(absu).path)
        name = sanitize_name(name)
        pdfs.add((name, absu))

    return list(pdfs)

# ------------------------------------------------------------
# Passo 2: Selenium (renderiza JS e rolagem)
# ------------------------------------------------------------
def scrape_with_selenium():
    print("🧭 [selenium] Renderizando página… (headless)")
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager

    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1440,900")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_opts)
    try:
        driver.get(BASE_URL)
        # espera básico
        time.sleep(4)

        # rolagem para carregar lazy content
        last_h = 0
        for _ in range(10):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")

        pdfs = set()

        # <a href="...pdf">
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                absu = to_abs(href)
                text = (a.get_text(strip=True) or os.path.basename(urlparse(absu).path))
                name = sanitize_name(text)
                if not name.lower().endswith(".pdf"):
                    name = os.path.basename(urlparse(absu).path) or (name + ".pdf")
                pdfs.add((name, absu))

        # regex extra
        for m in re.findall(r'(?i)href=["\']([^"\']+\.pdf[^"\']*)["\']', html):
            absu = to_abs(m)
            name = os.path.basename(urlparse(absu).path)
            name = sanitize_name(name)
            pdfs.add((name, absu))

        return list(pdfs)
    finally:
        driver.quit()

# ------------------------------------------------------------
# main
# ------------------------------------------------------------
def main(download=True):
    ensure_dirs()

    rows = scrape_with_requests()
    if not rows:
        rows = scrape_with_selenium()

    # dedup por URL mantendo o primeiro nome mais legível
    dedup = {}
    for name, url in rows:
        dedup.setdefault(url, name)
    rows = [(n, u) for u, n in dedup.items()]
    rows.sort(key=lambda x: x[0].lower())

    print(f"✅ Encontrados {len(rows)} PDFs")
    if not rows:
        print("ℹ️  Nada para salvar.")
        return

    write_csv(rows)
    print(f"💾 CSV salvo em: {CSV_PATH}")

    if download:
        ok = 0
        for name, url in rows:
            filename = name if name.lower().endswith(".pdf") else (name + ".pdf")
            if not filename.lower().endswith(".pdf"):
                filename += ".pdf"
            path = os.path.join(OUT_DIR, filename)
            # evita baixar de novo se já existe
            if os.path.exists(path) and os.path.getsize(path) > 0:
                print(f"↪️  Já existe: {filename}")
                ok += 1
                continue
            print(f"⬇️  Baixando: {filename}")
            if download_file(url, path):
                ok += 1
        print(f"🎉 Download concluído: {ok}/{len(rows)}")

if __name__ == "__main__":
    # para pular download, chame: python scripts/scraper_alter_material_venda.py nodl
    dl = not (len(sys.argv) > 1 and sys.argv[1].lower() in {"nodl", "no-download"})
    main(download=dl))
