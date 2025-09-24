#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawler Affix -> baixa PDFs públicos para data/affix/raw
Gera manifest.json + manifest.csv apenas com URLs 200 (application/pdf)
"""
import os, re, json, time, csv, hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
AFFIX_DIR = ROOT / "data" / "affix"
RAW_DIR = AFFIX_DIR / "raw"
OCR_DIR = AFFIX_DIR / "ocr"  # reservado
MANIFEST_JSON = AFFIX_DIR / "manifest.json"
MANIFEST_CSV = AFFIX_DIR / "manifest.csv"
SOURCES = ROOT / "scripts" / "affix_sources.txt"

session = requests.Session()
session.headers.update({
    "User-Agent": "CRION-AffixCrawler/1.3 (+GitHub Actions)",
    "Accept": "text/html,application/pdf"
})

YR = r"(20\d{2})"  # TODOS os anos 2000+ (ex.: 2015..2026)

PDF_RE = re.compile(r"\.pdf(?:$|\?)", re.I)
AFFIX_DOM = "affix.com.br"

def is_pdf_url(u: str) -> bool:
    try:
        p = urlparse(u)
        if not p.scheme.startswith("http"):
            return False
        if AFFIX_DOM not in p.netloc.lower():
            return False
    except Exception:
        return False
    return bool(PDF_RE.search(u))

def head_ok(u: str, timeout=25) -> bool:
    try:
        r = session.head(u, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return False
        ct = (r.headers.get("Content-Type") or "").lower()
        return "pdf" in ct or ct == ""
    except Exception:
        return False

def sanitize_name(url: str) -> str:
    path = urlparse(url).path
    name = os.path.basename(path)
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def fetch(url: str, timeout=40):
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.content
    except Exception:
        pass
    return None

def crawl_page(start_url: str, max_pages=500):
    out = set()
    seen = set([start_url])
    queue = [start_url]
    base_netloc = urlparse(start_url).netloc

    while queue and len(seen) <= max_pages:
        u = queue.pop(0)
        try:
            r = session.get(u, timeout=20)
            if r.status_code != 200:
                continue
            ct = (r.headers.get("Content-Type") or "").lower()
            if "text/html" not in ct:
                continue
            soup = BeautifulSoup(r.text, "lxml")
        except Exception:
            continue

        for a in soup.select("a[href]"):
            href = urljoin(u, a.get("href")).split("#")[0]
            if is_pdf_url(href):
                out.add(href)
                continue
            try:
                p = urlparse(href)
                if AFFIX_DOM in p.netloc.lower() and p.netloc == base_netloc:
                    if href not in seen:
                        seen.add(href)
                        queue.append(href)
            except Exception:
                pass

    return sorted(out)

def read_sources():
    urls = set()
    if SOURCES.exists():
        for raw in SOURCES.read_text("utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"): 
                continue
            if line.lower().startswith("crawl "):
                start = line.split(" ",1)[1].strip()
                for u in crawl_page(start):
                    if is_pdf_url(u):
                        urls.add(u)
            elif line.lower().startswith("link "):
                u = line.split(" ",1)[1].strip()
                if is_pdf_url(u):
                    urls.add(u)
    return sorted(urls)

def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OCR_DIR.mkdir(parents=True, exist_ok=True)

def parse_year(u: str) -> str:
    m = re.search(YR, u)
    return m.group(1) if m else ""

UF_RE = re.compile(r"-(AC|AL|AM|AP|BA|CE|DF|ES|GO|MA|MG|MS|MT|PA|PB|PE|PI|PR|RJ|RN|RO|RR|RS|SC|SE|SP|TO)-", re.I)
def parse_uf(u: str) -> str:
    m = UF_RE.search(u)
    return m.group(1).upper() if m else ""

def main():
    ensure_dirs()
    urls = set(read_sources())

    # Merge inicial com CSV existente (mantém seus testes manuais)
    if MANIFEST_CSV.exists():
        for row in csv.DictReader(MANIFEST_CSV.read_text("utf-8").splitlines()):
            u = (row.get("url") or "").strip()
            if is_pdf_url(u):
                urls.add(u)

    valid = []
    for u in sorted(urls):
        if not head_ok(u):
            continue
        name = sanitize_name(u)
        dest = RAW_DIR / name
        if not dest.exists():
            data = fetch(u)
            if not data:
                continue
            dest.write_bytes(data)
            time.sleep(0.3)
        size = dest.stat().st_size
        valid.append({
            "name": name,
            "url": u,
            "size": size,
            "year": parse_year(u),
            "state": parse_uf(u),
            "source": "affix",
        })

    # manifest.json
    MANIFEST_JSON.write_text(
        json.dumps({
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "base": "./data/affix/raw/",
            "items": [{"name": v["name"], "size": v["size"]} for v in sorted(valid, key=lambda x: x["name"].lower())]
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # manifest.csv
    with MANIFEST_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name","url","year","state","source"])
        for v in sorted(valid, key=lambda x: x["name"].lower()):
            w.writerow([v["name"], v["url"], v["year"], v["state"], v["source"]])

    print(f"OK: {len(valid)} PDFs válidos.")

if __name__ == "__main__":
    main())
