#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, time, hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "affix" / "raw"
MANIFEST = ROOT / "data" / "affix" / "manifest.json"
SOURCES = ROOT / "scripts" / "affix_sources.txt"

UA = "CRION-AffixCrawler/1.0 (+GitHub Actions)"
session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept": "text/html,application/pdf"})

def is_pdf_2025(url: str) -> bool:
    u = url.split("#")[0].split("?")[0]
    if not u.lower().endswith(".pdf"):
        return False
    # precisa ser do domínio affix.com.br
    try:
        netloc = urlparse(u).netloc.lower()
        if not netloc.endswith("affix.com.br"):
            return False
    except Exception:
        return False
    return ("/2025/" in u) or bool(re.search(r"-25\.pdf$", u, re.I))

def sanitize_name(url: str) -> str:
    name = os.path.basename(urlparse(url).path)
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def fetch(url: str, timeout=25):
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r
    except Exception:
        pass
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
        r = fetch(u, timeout=20)
        if not r or "text/html" not in (r.headers.get("Content-Type") or ""):
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href]"):
            href = urljoin(u, a.get("href"))
            href = href.split("#")[0]
            if is_pdf_2025(href):
                out.add(href)
                continue
            try:
                p = urlparse(href)
                if p.netloc.endswith("affix.com.br"):
                    queue.append(href)
            except Exception:
                pass
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
            start = line.split(" ",1)[1].strip()
            for u in crawl(start):
                pdfs.add(u)
        elif line.lower().startswith("link "):
            u = line.split(" ",1)[1].strip()
            if is_pdf_2025(u):
                pdfs.add(u)
    return sorted(pdfs)

def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "affix").mkdir(parents=True, exist_ok=True)

def download_all(urls):
    rows = []
    for u in urls:
        name = sanitize_name(u)
        dest = RAW_DIR / name
        need = (not dest.exists())
        if not need:
            # pequeno ETag local por tamanho+md5 de URL
            pass
        if need:
            r = fetch(u, timeout=40)
            if not r or not r.content:
                print(f"falhou: {u}")
                continue
            dest.write_bytes(r.content)
            print("baixado:", name, len(r.content), "bytes")
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
        print("manifest atualizado:", MANIFEST)
    else:
        print("manifest sem mudanças")

def main():
    ensure_dirs()
    urls = read_sources()
    if not urls:
        print("Nenhum PDF 2025 encontrado a partir das fontes.")
        write_manifest([])
        return
    items = download_all(urls)
    write_manifest(items)

if __name__ == "__main__":
    main()
