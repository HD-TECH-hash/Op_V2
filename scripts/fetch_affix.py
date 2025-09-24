#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, time, csv, sys, traceback
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "affix" / "raw"
MANIFEST_JSON = ROOT / "data" / "affix" / "manifest.json"
MANIFEST_CSV  = ROOT / "data" / "affix" / "manifest.csv"
SOURCES_TXT   = ROOT / "scripts" / "affix_sources.txt"

UA = "CRION-AffixCrawler/1.0 (+GitHub Actions)"
session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept": "text/html,application/pdf"})

YR = r"(?:20\d{2})"  # todos os anos 2000+
DOMAIN_OK = "affix.com.br"

def is_pdf(url: str) -> bool:
    u = url.split("#")[0].split("?")[0]
    return u.lower().endswith(".pdf")

def belongs(url: str) -> bool:
    try:
        return urlparse(url).netloc.lower().endswith(DOMAIN_OK)
    except Exception:
        return False

def sanitize_name(url: str) -> str:
    name = os.path.basename(urlparse(url).path)
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def fetch(url: str, timeout=40):
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r
        print(f"[warn] HTTP {r.status_code} -> {url}")
    except Exception as e:
        print(f"[warn] fetch fail: {url} -> {e}")
    return None

def crawl(start_url: str, max_pages=500):
    seen = set()
    queue = [start_url]
    out = set()
    while queue and len(seen) < max_pages:
        u = queue.pop(0)
        if u in seen:
            continue
        seen.add(u)
        r = fetch(u, timeout=20)
        ct = (r.headers.get("Content-Type") or "") if r else ""
        if not r or ("text/html" not in ct and "application/xhtml" not in ct):
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href]"):
            href = urljoin(u, a.get("href"))
            href = href.split("#")[0]
            if is_pdf(href) and belongs(href):
                out.add(href)
                continue
            try:
                p = urlparse(href)
                if p.netloc and p.netloc.lower().endswith(DOMAIN_OK):
                    queue.append(href)
            except Exception:
                pass
    return sorted(out)

def load_sources():
    urls = set()
    # 1) seeds do TXT
    if SOURCES_TXT.exists():
        for raw in SOURCES_TXT.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("crawl "):
                start = line.split(" ",1)[1].strip()
                for u in crawl(start):
                    if is_pdf(u) and belongs(u):
                        urls.add(u)
            elif line.lower().startswith("link "):
                u = line.split(" ",1)[1].strip()
                if is_pdf(u) and belongs(u):
                    urls.add(u)
    # 2) seeds do CSV
    if MANIFEST_CSV.exists():
        with MANIFEST_CSV.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                u = (row.get("url") or "").strip()
                if is_pdf(u) and belongs(u):
                    urls.add(u)
    return sorted(urls)

def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "affix").mkdir(parents=True, exist_ok=True)

def download_all(urls):
    rows = []
    for u in urls:
        name = sanitize_name(u)
        dest = RAW_DIR / name
        need = not dest.exists() or dest.stat().st_size == 0
        if need:
            r = fetch(u, timeout=60)
            if not r or not r.content:
                print(f"[warn] falhou download: {u}")
                continue
            dest.write_bytes(r.content)
            print(f"[ok] baixado: {name} ({len(r.content)} bytes)")
            time.sleep(0.2)
        size = dest.stat().st_size if dest.exists() else 0
        rows.append({"name": name, "size": size, "url": u})
    return rows

def write_manifest(items):
    items = sorted(items, key=lambda x: x["name"].lower())
    data = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base": "./data/affix/raw/",
        "items": items
    }
    new = json.dumps(data, ensure_ascii=False, indent=2)
    MANIFEST_JSON.parent.mkdir(parents=True, exist_ok=True)
    old = MANIFEST_JSON.read_text("utf-8") if MANIFEST_JSON.exists() else ""
    if new != old:
        MANIFEST_JSON.write_text(new, encoding="utf-8")
        print(f"[ok] manifest atualizado: {MANIFEST_JSON}")
    else:
        print("[ok] manifest sem mudanças")

def main():
    ensure_dirs()
    urls = load_sources()
    if not urls:
        print("[info] Nenhuma URL encontrada (seeds vazios ou bloqueados).")
        write_manifest([])
        return
    items = download_all(urls)
    write_manifest(items)

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except SystemExit as se:
        raise
    except Exception:
        print("[fatal] erro inesperado no crawler:")
        traceback.print_exc()
        # não derruba o job – registra e segue
        sys.exit(0)
