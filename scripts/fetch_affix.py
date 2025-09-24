#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, time, hashlib, sys
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "affix" / "raw"
MANIFEST = ROOT / "data" / "affix" / "manifest.json"
SOURCES = ROOT / "scripts" / "affix_sources.txt"
LOGFILE = ROOT / "scripts" / "affix_last_run.log"

UA = "CRION-AffixCrawler/1.1 (+GitHub Actions)"
session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept": "text/html,application/pdf"})

def log(*a):
    msg = " ".join(map(str, a))
    print(msg, flush=True)
    try:
        with LOGFILE.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

# aceita PDFs do domínio affix.com.br, priorizando 2025 (padrão -25.pdf ou /2025/)
YR = r"(?:2025)"
def is_pdf_ok(url: str) -> bool:
    u = url.split("#")[0].split("?")[0]
    if not u.lower().endswith(".pdf"):
        return False
    try:
        netloc = urlparse(u).netloc.lower()
        if not netloc.endswith("affix.com.br"):
            return False
    except Exception:
        return False
    return (f"/{YR}/" in u) or bool(re.search(rf"-{YR[-2:]}\.pdf$", u, re.I))

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
        log("HTTP", r.status_code, url)
    except Exception as e:
        log("REQ_FAIL", url, e)
    return None

def crawl(start_url: str, max_pages=600):
    seen = set()
    queue = [start_url]
    out = set()
    domain = urlparse(start_url).netloc.lower()

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
            if is_pdf_ok(href):
                out.add(href)
                continue
            try:
                p = urlparse(href)
                if p.netloc.lower().endswith("affix.com.br") and p.netloc.lower() == domain:
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
            log("CRAWL", start)
            for u in crawl(start):
                pdfs.add(u)
        elif line.lower().startswith("link "):
            u = line.split(" ",1)[1].strip()
            if is_pdf_ok(u):
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
        if need:
            r = fetch(u, timeout=40)
            if not r or not r.content:
                log("DL_FAIL", u)
                continue
            dest.write_bytes(r.content)
            log("DOWN", name, len(r.content), "bytes")
            time.sleep(0.3)
        size = dest.stat().st_size if dest.exists() else 0
        rows.append({"name": name, "size": size, "url": u})
    return rows

def write_manifest(items):
    items = sorted(items, key=lambda x: x["name"].lower())
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base": "./data/affix/raw/",
        "items": items
    }
    new = json.dumps(data, ensure_ascii=False, indent=2)
    MANIFEST.write_text(new, encoding="utf-8")
    log("MANIFEST", MANIFEST)

def main():
    LOGFILE.write_text("", encoding="utf-8")
    ensure_dirs()
    urls = read_sources()
    if not urls:
        log("NO_URLS_FOUND")
        write_manifest([])
        sys.exit(0)
    items = download_all(urls)
    write_manifest(items)

if __name__ == "__main__":
    main()
