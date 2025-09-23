#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baixa PDFs da Affix a partir de scripts/affix_sources.txt e grava:
- data/affix/raw/<arquivo>.pdf
- data/affix/manifest.json  (lista simples usada pelo index.html)

Formato do manifest:
{
  "items": [
    {
      "name": "Affix-...pdf",
      "url": "https://....pdf",
      "status": "ok|unavailable|error: ...",
      "size": 2209576,
      "timestamp": 1758586482,
      "repo_raw": "https://raw.githubusercontent.com/.../data/affix/raw/Affix-...pdf"  # se baixado
    }
  ]
}
"""
import os, re, sys, json, hashlib, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = os.path.dirname(os.path.dirname(__file__))            # .../scripts -> repo root
RAW_DIR = os.path.join(ROOT, "data", "affix", "raw")
OUT_MANIFEST = os.path.join(ROOT, "data", "affix", "manifest.json")
SRC_FILE = os.path.join(ROOT, "scripts", "affix_sources.txt")

HEADERS = {
    "User-Agent": "RICAI-AFFIX/1.1 (+github actions)",
    "Accept": "application/pdf, text/html;q=0.8, */*;q=0.5",
}
TIMEOUT = 45

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_MANIFEST), exist_ok=True)

def is_pdf_url(url: str) -> bool:
    return url.lower().split("?", 1)[0].endswith(".pdf")

def sanitize_name(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or ("file-"+hashlib.md5(url.encode()).hexdigest()+".pdf")
    name = re.sub(r"[^A-Za-z0-9._\-]+", "_", name)
    return name[:180]

def head_ok(url: str) -> bool:
    try:
        r = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=TIMEOUT)
        if r.status_code == 405:  # alguns hosts bloqueiam HEAD
            r = requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT)
            ok = (r.status_code == 200) and (is_pdf_url(url) or "pdf" in (r.headers.get("Content-Type","").lower()))
            r.close()
            return ok
        ct = (r.headers.get("Content-Type","") or "").lower()
        return (r.status_code == 200) and ("pdf" in ct or is_pdf_url(url))
    except Exception:
        return False

def fetch_pdf(url: str, dest: str) -> int:
    with requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1024*64):
                if chunk: f.write(chunk)
    return os.path.getsize(dest)

def crawl(base_url: str):
    try:
        r = requests.get(base_url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            absu = urljoin(base_url, a["href"])
            if is_pdf_url(absu): links.append(absu)
        # dedup mantendo ordem
        seen, out = set(), []
        for u in links:
            if u not in seen:
                seen.add(u); out.append(u)
        return out
    except Exception:
        return []

def read_sources():
    seeds = []
    if not os.path.exists(SRC_FILE): return seeds
    with open(SRC_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#"): continue
            m = re.match(r"^(link|crawl)\s+(https?://\S+)$", line, flags=re.I)
            if not m: continue
            seeds.append((m.group(1).lower(), m.group(2)))
    return seeds

def main():
    ensure_dirs()
    only_2025 = os.environ.get("AFFIX_ONLY_2025","0") == "1"

    items = []
    seen_names = set()

    for kind, base in read_sources():
        urls = [base] if kind == "link" else crawl(base)
        for url in urls:
            if only_2025:
                # mantém PDFs que contem "-25.pdf" ou "/2025/"
                if ("-25.pdf" not in url) and ("/2025/" not in url):
                    continue
            if not is_pdf_url(url): 
                continue

            name = sanitize_name(url)
            if name in seen_names: 
                continue
            seen_names.add(name)

            row = {
                "name": name,
                "url": url,
                "status": "pending",
                "size": 0,
                "timestamp": int(time.time()),
            }

            if head_ok(url):
                dest = os.path.join(RAW_DIR, name)
                try:
                    size = fetch_pdf(url, dest)
                    row["status"] = "ok"
                    row["size"] = size
                    row["repo_raw"] = f"https://raw.githubusercontent.com/HD-TECH-hash/Ricai-crion-operacao/main/data/affix/raw/{name}"
                except Exception as e:
                    row["status"] = f"error: {e.__class__.__name__}"
            else:
                row["status"] = "unavailable"

            items.append(row)

    with open(OUT_MANIFEST, "w", encoding="utf-8") as f:
        json.dump({"items": items, "generated_at": int(time.time())}, f, ensure_ascii=False, indent=2)

    ok = sum(1 for x in items if x["status"]=="ok")
    bad = sum(1 for x in items if x["status"]!="ok")
    print(f"OK: {ok} • indisponíveis/erros: {bad}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
