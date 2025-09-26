# scripts/scraper_alter_material_venda.py
# -*- coding: utf-8 -*-

import os, re, csv, sys, time
from urllib.parse import urljoin, urlparse

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

def get_driver():
    # Selenium 4.x já baixa o driver via selenium-manager (dispensa webdriver-manager)
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    headless = os.environ.get("HEADLESS", "1") != "0"

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1440,1200")
    opts.add_argument("--lang=pt-BR")
    opts.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(5)
    return driver

def maybe_accept_cookie(driver):
    from selenium.webdriver.common.by import By
    candidates = [
        "//button[contains(translate(., 'ACEITAROK', 'aceitarok'), 'aceitar')]",
        "//button[contains(translate(., 'ACEITAROK', 'aceitarok'), 'ok')]",
        "//button[contains(., 'Aceitar')]",
        "//button[contains(., 'OK')]",
        "//div[contains(@class,'cookie')]//button",
        "//a[contains(@class,'accept')]",
    ]
    for xp in candidates:
        try:
            el = driver.find_element(By.XPATH, xp)
            if el.is_displayed():
                el.click()
                time.sleep(0.8)
                return
        except Exception:
            pass

def scroll_to_bottom(driver, max_loops=16, pause=1.0):
    last_h = 0
    for _ in range(max_loops):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        h = driver.execute_script("return document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

def expand_sections(driver):
    # Tenta expandir acordeões/tabs comuns em temas WP/Elementor
    from selenium.webdriver.common.by import By
    xps = [
        "//div[contains(@class,'accordion')]//button",
        "//div[contains(@class,'elementor-accordion')]//div[contains(@class,'elementor-tab-title')]",
        "//div[contains(@class,'wp-block-accordion')]//button",
        "//button[contains(@class,'accordion-button')]",
        "//div[contains(@class,'tabs')]//a[contains(@class,'tab')]",
        "//div[contains(@class,'elementor-toggle')]//div[contains(@class,'elementor-tab-title')]",
    ]
    for xp in xps:
        try:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns[:200]:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                    time.sleep(0.2)
                    b.click()
                    time.sleep(0.2)
                except Exception:
                    pass
        except Exception:
            pass

def extract_pdfs_from_html(html):
    found = set()
    # 1) <a href="...pdf">
    for m in re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, flags=re.I):
        url = to_abs(m.strip())
        name = sanitize(os.path.basename(urlparse(url).path))
        found.add((name, url))
    # 2) URLs .pdf perdidas em JS/dados
    for m in re.findall(r'(https?://[^\s\'"]+\.pdf[^\s\'"]*)', html, flags=re.I):
        url = to_abs(m.strip())
        name = sanitize(os.path.basename(urlparse(url).path))
        found.add((name, url))
    return list(found)

def main(download=True):
    ensure_dirs()
    print("🔎 Acessando página...")
    from selenium.webdriver.common.by import By
    driver = get_driver()
    try:
        driver.get(BASE_URL)
        time.sleep(2.0)
        maybe_accept_cookie(driver)
        # algumas páginas carregam via scroll/accordion
        scroll_to_bottom(driver, max_loops=20, pause=0.8)
        expand_sections(driver)
        scroll_to_bottom(driver, max_loops=8, pause=0.6)

        html = driver.page_source

        # debug opcional
        with open("data/affix/alter_debug.html","w",encoding="utf-8") as f:
            f.write(html)

        rows = extract_pdfs_from_html(html)

        # dedup por URL
        dedup = {}
        for name, url in rows:
            dedup[url] = dedup.get(url) or name
        rows = [(n, u) for u, n in dedup.items()]

        print(f"✅ Encontrados {len(rows)} PDFs")
        if not rows:
            print("⚠️ Nada encontrado. Veja data/affix/alter_debug.html para inspecionar o HTML renderizado.")
            return

        # ordena por nome
        rows.sort(key=lambda x: x[0].lower())
        save_csv(rows)

        if not download:
            return

        # download
        import requests
        for name, url in rows:
            fname = name if name.lower().endswith(".pdf") else name + ".pdf"
            path = os.path.join(OUT_DIR, fname)
            if os.path.exists(path) and os.path.getsize(path) > 0:
                print(f"↪️ já existe: {fname}")
                continue
            print(f"⬇️ baixando: {fname}")
            try:
                with requests.get(url, stream=True, timeout=90, headers={"User-Agent":"Mozilla/5.0"}) as r:
                    r.raise_for_status()
                    with open(path, "wb") as f:
                        for chunk in r.iter_content(1<<14):
                            if chunk: f.write(chunk)
            except Exception as e:
                print(f"  ⚠️ falhou: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    dl = not (len(sys.argv) > 1 and sys.argv[1].lower() in {"nodl","no-download"})
    main(download=dl)
