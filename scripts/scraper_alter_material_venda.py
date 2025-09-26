# scripts/scraper_alter_material_venda.py
# -*- coding: utf-8 -*-

import os, re, csv, sys, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.alter.com.br/portal-do-parceiro/material-de-venda/"
OUT_DIR  = "data/affix/raw"
CSV_PATH = "data/affix/alter_pdfs_manifest.csv"

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)

def to_abs(url, base=BASE_URL):
    return url if urlparse(url).netloc else urljoin(base, url)

def sanitize(name):
    name = re.sub(r'[\\/:*?"<>|#]+', "-", (name or "").strip())
    return name or "arquivo.pdf"

def save_csv(rows):
    ensure_dirs()
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["name","url"]); w.writerows(rows)
    print(f"💾 CSV salvo em: {CSV_PATH}")

def scrape_requests():
    print("🔎 [requests] Acessando página…")
    r = requests.get(BASE_URL, timeout=60, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "lxml")
    found = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            url = to_abs(href)
            text = a.get_text(strip=True) or os.path.basename(urlparse(url).path)
            name = sanitize(text)
            if not name.lower().endswith(".pdf"):
                name = os.path.basename(urlparse(url).path) or (name + ".pdf")
            found.add((name, url))

    # Regex extra (links em atributos/JS inline)
    for m in re.findall(r'(?i)href=["\']([^"\']+\.pdf[^"\']*)["\']', html):
        url = to_abs(m)
        name = sanitize(os.path.basename(urlparse(url).path))
        found.add((name, url))

    return list(found)

def scrape_selenium():
    print("🧭 [selenium] Renderizando (headless)…")
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1440,900")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
    try:
        driver.get(BASE_URL)
        time.sleep(4)  # aguarde carregar elementos

        # rolar para carregar lazy content
        last = 0
        for _ in range(12):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last: break
            last = h

        html = driver.page_source
        # DEBUG opcional: salvar HTML para inspecionar
        with open("data/affix/alter_debug.html","w",encoding="utf-8") as f:
            f.write(html)

        soup = BeautifulSoup(html, "lxml")
        found = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                url = to_abs(href)
                text = a.get_text(strip=True) or os.path.basename(urlparse(url).path)
                name = sanitize(text)
                if not name.lower().endswith(".pdf"):
                    name = os.path.basename(urlparse(url).path) or (name + ".pdf")
                found.add((name, url))

        for m in re.findall(r'(?i)href=["\']([^"\']+\.pdf[^"\']*)["\']', html):
            url = to_abs(m)
            name = sanitize(os.path.basename(urlparse(url).path))
            found.add((name, url))

        return list(found)
    finally:
        driver.quit()

def main(download=True):
    ensure_dirs()
    rows = scrape_requests()
    if not rows:
        try:
            rows = scrape_selenium()
        except Exception as e:
            print(f"⚠️ Selenium falhou: {e}")

    # dedup por URL
    dedup = {}
    for name, url in rows:
        dedup.setdefault(url, name)
    rows = [(n, u) for u, n in dedup.items()]
    rows.sort(key=lambda x: x[0].lower())

    print(f"✅ Encontrados {len(rows)} PDFs")
    if not rows:
        return

    save_csv(rows)

    if not download:
        return

    # download opcional
    for name, url in rows:
        fname = name if name.lower().endswith(".pdf") else name + ".pdf"
        path = os.path.join(OUT_DIR, fname)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            print(f"↪️ já existe: {fname}")
            continue
        print(f"⬇️ baixando: {fname}")
        try:
            with requests.get(url, stream=True, timeout=90) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(1<<14):
                        if chunk: f.write(chunk)
        except Exception as e:
            print(f"  ⚠️ falhou: {e}")

if __name__ == "__main__":
    dl = not (len(sys.argv) > 1 and sys.argv[1].lower() in {"nodl","no-download"})
    main(download=dl)
