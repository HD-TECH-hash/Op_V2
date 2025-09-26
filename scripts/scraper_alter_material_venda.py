# scripts/scraper_alter_material_venda.py
# -*- coding: utf-8 -*-

import os, re, csv, sys, time, queue, hashlib
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

BASE_START = "https://www.alter.com.br/portal-do-parceiro/material-de-venda/"
ALLOW_NETLOC = "www.alter.com.br"  # restringe o domínio
MAX_DEPTH = int(os.environ.get("ALTER_MAX_DEPTH", "2"))  # crawl até 2 cliques
TIMEOUT = 30

OUT_DIR  = "data/affix/raw"
CSV_PATH = "data/affix/alter_pdfs_manifest.csv"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)

def norm_url(u, base):
    if not u:
        return None
    u = urldefrag(u)[0].strip()  # remove #ancora
    if not u:
        return None
    if not urlparse(u).netloc:
        u = urljoin(base, u)
    # só segue dentro do domínio permitido
    if urlparse(u).netloc and urlparse(u).netloc != ALLOW_NETLOC:
        return None
    return u

def looks_like_pdf_url(u: str) -> bool:
    return bool(re.search(r"\.pdf(\?|#|$)", u, flags=re.I))

def extract_pdf_pairs_from_html(html: str, base: str):
    pairs = set()

    # 1) hrefs diretos
    for m in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I):
        url = norm_url(m, base)
        if not url: continue
        if looks_like_pdf_url(url):
            name = os.path.basename(urlparse(url).path) or hashlib.md5(url.encode()).hexdigest()+".pdf"
            pairs.add((name, url))

    # 2) URLs soltas/JS
    for m in re.findall(r'(https?://[^\s\'"]+)', html, flags=re.I):
        url = norm_url(m, base)
        if not url: continue
        if looks_like_pdf_url(url):
            name = os.path.basename(urlparse(url).path) or hashlib.md5(url.encode()).hexdigest()+".pdf"
            pairs.add((name, url))

    return pairs

def resolve_pdf_redirect(u: str):
    """Segue redirects; se final é PDF (content-type) ou termina com .pdf, retorna URL final."""
    try:
        # Primeiro um HEAD (alguns servidores não suportam HEAD — tente GET pequeno)
        r = SESS.head(u, allow_redirects=True, timeout=TIMEOUT)
        final = r.url
        ctype = r.headers.get("Content-Type","").lower()
        if "application/pdf" in ctype or looks_like_pdf_url(final):
            return final

        # fallback GET leve só para headers
        r = SESS.get(u, stream=True, allow_redirects=True, timeout=TIMEOUT)
        final = r.url
        ctype = r.headers.get("Content-Type","").lower()
        if "application/pdf" in ctype or looks_like_pdf_url(final):
            return final
    except Exception:
        pass
    return None

def collect_internal_links(html: str, base: str):
    links = set()
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = norm_url(a.get("href"), base)
        if not href: continue
        # ignora tel:, mailto:, etc.
        if href.startswith(("mailto:","tel:","javascript:")):
            continue
        links.add(href)
    return links

def selenium_get_html(url: str):
    """Usa Selenium para renderizar e retornar page_source (aceita cookies, scroll, expande)."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By

    opts = Options()
    if os.environ.get("HEADLESS","1") != "0":
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1440,1200")
    opts.add_argument("--lang=pt-BR")
    opts.add_argument(f"--user-agent={UA}")

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(5)
    try:
        driver.get(url)
        time.sleep(2)

        # tenta aceitar cookies
        candidates = [
            "//button[contains(translate(., 'ACEITAROK', 'aceitarok'), 'aceitar')]",
            "//button[contains(., 'Aceitar')]",
            "//button[contains(., 'OK')]",
            "//div[contains(@class,'cookie')]//button",
            "//a[contains(@class,'accept')]",
        ]
        for xp in candidates:
            try:
                el = driver.find_element(By.XPATH, xp)
                if el.is_displayed(): el.click(); time.sleep(0.5); break
            except Exception:
                pass

        # scroll para carregar lazy
        last_h = 0
        for _ in range(14):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.7)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last_h: break
            last_h = h

        # expande acordeões comuns
        expand_xps = [
            "//div[contains(@class,'accordion')]//button",
            "//div[contains(@class,'elementor-accordion')]//div[contains(@class,'elementor-tab-title')]",
            "//button[contains(@class,'accordion-button')]",
            "//div[contains(@class,'elementor-toggle')]//div[contains(@class,'elementor-tab-title')]",
        ]
        for xp in expand_xps:
            try:
                for el in driver.find_elements(By.XPATH, xp)[:200]:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        time.sleep(0.1)
                        el.click()
                        time.sleep(0.2)
                    except Exception:
                        pass
            except Exception:
                pass

        html = driver.page_source
        return html
    finally:
        driver.quit()

def crawl_and_collect(start_url: str):
    """Crawl em largura até MAX_DEPTH, coletando PDFs diretos e por redirecionamento."""
    seen_pages = set()
    q = queue.Queue()
    q.put((start_url, 0))
    seen_pages.add(start_url)

    pdf_pairs = set()

    # 1) pega HTML renderizado da página inicial com Selenium (pega conteúdo dinâmico)
    print("🔎 Acessando página (Selenium)...")
    try:
        start_html = selenium_get_html(start_url)
    except Exception as e:
        print(f"⚠️ Selenium falhou: {e}. Tentando via requests…")
        start_html = SESS.get(start_url, timeout=TIMEOUT).text

    # salva debug
    ensure_dirs()
    with open("data/affix/alter_debug_start.html","w",encoding="utf-8") as f:
        f.write(start_html or "")

    # PDFs diretos já nessa página
    pdf_pairs |= extract_pdf_pairs_from_html(start_html, start_url)

    # Enfileira links internos encontrados na inicial
    internal_links = collect_internal_links(start_html, start_url)
    for u in internal_links:
        if u not in seen_pages:
            q.put((u, 1))
            seen_pages.add(u)

    # 2) BFS nas próximas páginas via requests (mais leve/rápido)
    while not q.empty():
        url, depth = q.get()
        if depth > MAX_DEPTH:
            continue
        try:
            r = SESS.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            html = r.text
        except Exception as e:
            # tenta uma passada com selenium se a página parece vazia
            try:
                html = selenium_get_html(url)
            except Exception:
                # desiste
                continue

        # salva debugs leves para inspeção (levando hash no nome)
        h = hashlib.md5(url.encode()).hexdigest()[:8]
        with open(f"data/affix/alter_dbg_{depth}_{h}.html","w",encoding="utf-8") as f:
            f.write(html or "")

        # PDFs diretos
        pdf_pairs |= extract_pdf_pairs_from_html(html, url)

        # Links candidatos que podem redirecionar a PDF (sem .pdf explícito)
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            cand = norm_url(a.get("href"), url)
            if not cand: continue

            # Se já é PDF, já pegamos acima; aqui tentamos redirecionamento
            if not looks_like_pdf_url(cand):
                final = resolve_pdf_redirect(cand)
                if final and looks_like_pdf_url(final):
                    name = os.path.basename(urlparse(final).path) or hashlib.md5(final.encode()).hexdigest()+".pdf"
                    pdf_pairs.add((name, final))

        # amplia o crawl (apenas se ainda na profundidade)
        if depth < MAX_DEPTH:
            for u in collect_internal_links(html, url):
                if u not in seen_pages:
                    seen_pages.add(u)
                    q.put((u, depth+1))

    # dedup por URL final
    dedup = {}
    for name, url in pdf_pairs:
        dedup[url] = dedup.get(url) or name
    out = [(n, u) for u, n in dedup.items()]
    out.sort(key=lambda x: x[0].lower())
    return out

def save_csv(rows):
    ensure_dirs()
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name","url"])
        for n,u in rows:
            w.writerow([n,u])
    print(f"💾 CSV salvo em: {CSV_PATH}")

def download_all(rows):
    ensure_dirs()
    for name, url in rows:
        fname = name if name.lower().endswith(".pdf") else name + ".pdf"
        path = os.path.join(OUT_DIR, fname)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            print(f"↪️ já existe: {fname}")
            continue
        print(f"⬇️ baixando: {fname}")
        try:
            with SESS.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(1<<14):
                        if chunk: f.write(chunk)
        except Exception as e:
            print(f"  ⚠️ falhou: {e}")

def main(download=True):
    ensure_dirs()
    rows = crawl_and_collect(BASE_START)
    print(f"✅ Encontrados {len(rows)} PDFs")
    if not rows:
        print("⚠️ Nada encontrado. Olhe os arquivos data/affix/alter_debug_start.html e data/affix/alter_dbg_*.html.")
        return
    save_csv(rows)
    if download:
        download_all(rows)

if __name__ == "__main__":
    dl = not (len(sys.argv)>1 and sys.argv[1].lower() in {"nodl","no-download"})
    main(download=dl)
