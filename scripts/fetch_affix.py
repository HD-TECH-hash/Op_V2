#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, time
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "affix" / "raw"
MANIFEST = ROOT / "data" / "affix" / "manifest.json"
SOURCES = ROOT / "scripts" / "affix_sources.txt"

# UA de navegador para evitar bloqueio por WordPress/CDN
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
session = requests.Session()
session.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/pdf",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
})

def is_affix(u: str) -> bool:
    try:
        return urlparse(u).netloc.lower().endswith("affix.com.br")
    except Exception:
        return False

def is_pdf_2025(url: str) -> bool:
    """PDF do domínio Affix e com 2025 em qualquer parte do path ou nome."""
    u = url.split("#")[0].split("?")[0]
    if not u.lower().endswith(".pdf"):
        return False
    if not is_affix(u):
        return False
    return "/2025/" in u or re.search(r"2025.*\.pdf$", u, re.I) is not None

def sanitize_name(url: str) -> str:
    name = os.path.basename(urlparse(url).path)
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def get(url: str, timeout=25, tries=3):
    for attempt in range(1, tries+1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r
            time.sleep(0.4*attempt)
        except Exception:
            time.sleep(0.4*attempt)
    return None

def crawl(start_url: str, max_pages=600):
    seen = set()
    queue = [start_url]
    out = set()
    while queue and len(seen) < max_pages:
        u = queue.pop(0)
        if u in seen:
            continue
        seen.add(u)
        r = get(u, timeout=20)
        if not r:
            continue
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ctype:
            # se não é HTML, ignora (PDF já é pego pelo link)
            continue

        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href]"):
            href = urljoin(u, a.get("href") or "")
            href = href.split("#")[0]
            if is_pdf_2025(href):
                out.add(href)
                continue
            # seguir apenas dentro do domínio affix
            try:
                if is_affix(href):
                    queue.append(href)
            except Exception:
                pass
    print(f"[crawl] {start_url} -> {len(out)} PDFs 2025")
    return sorted(out)

def read_sources():
    pdfs = set()
    if not SOURCES.exists():
        return []
    for raw in SOURCES.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("crawl "):
            start = line.split(" ", 1)[1].strip()
            for u in crawl(start):
                pdfs.add(u)
        elif line.lower().startswith("link "):
            u = line.split(" ", 1)[1].strip()
            if is_pdf_2025(u):
                pdfs.add(u)
    print(f"[sources] total PDFs 2025: {len(pdfs)}")
    return sorted(pdfs)

def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "affix").mkdir(parents=True, exist_ok=True)

def download_all(urls):
    rows = []
    for u in urls:
        name = sanitize_name(u)
        dest = RAW_DIR / name
        need = not dest.exists()
        if need:
            r = get(u, timeout=40)
            if not r or not r.content:
                print(f"[download] falhou: {u}")
                continue
            dest.write_bytes(r.content)
            print(f"[download] {name} ({len(r.content)} bytes)")
            time.sleep(0.3)
        size = dest.stat().st_size if dest.exists() else 0
        rows.append({"name": name, "size": size})
    return rows

def write_manifest(items):
    items = sorted(items, key=lambda x: x["name"].lower())
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base": "./data/affix/raw/",
        "items": items
    }
    old = MANIFEST.read_text("utf-8") if MANIFEST.exists() else ""
    new = json.dumps(data, ensure_ascii=False, indent=2)
    if new != old:
        MANIFEST.write_text(new, encoding="utf-8")
        print("[manifest] atualizado")
    else:
        print("[manifest] sem mudanças")

def main():
    ensure_dirs()
    urls = read_sources()
    if not urls:
        print("[main] Nenhum PDF 2025 encontrado a partir das fontes.")
        write_manifest([])
        return
    items = download_all(urls)
    write_manifest(items)

if __name__ == "__main__":
    main()
