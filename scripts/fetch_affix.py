#!/usr/bin/env python3
import os, re, sys, json, hashlib, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = os.path.dirname(os.path.dirname(__file__))  # .../scripts -> repo root
RAW_DIR = os.path.join(ROOT, "data", "affix", "raw")
OUT_MANIFEST = os.path.join(ROOT, "data", "affix", "manifest.json")
SRC_FILE = os.path.join(ROOT, "scripts", "affix_sources.txt")

HEADERS = {"User-Agent": "RICAI-AFFIX/1.0 (+github actions)"}
TIMEOUT = 40

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)

def is_pdf_url(url):
    return url.lower().endswith(".pdf")

def sanitize_name(url):
    name = os.path.basename(urlparse(url).path) or ("file-"+hashlib.md5(url.encode()).hexdigest()+".pdf")
    # nomes longos quebram a UI — corta e remove caracteres problemáticos
    name = re.sub(r"[^A-Za-z0-9._\-]+", "_", name)
    return name[:180]

def head_ok(url):
    try:
        r = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=TIMEOUT)
        ct = (r.headers.get("Content-Type","") or "").lower()
        return (r.status_code == 200) and ("pdf" in ct or url.lower().endswith(".pdf"))
    except Exception:
        return False

def fetch_pdf(url, dest):
    r = requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(1024*64):
            if chunk: f.write(chunk)
    size = os.path.getsize(dest)
    return size

def crawl(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            absu = urljoin(url, href)
            if is_pdf_url(absu):
                links.append(absu)
        return list(dict.fromkeys(links))
    except Exception:
        return []

def read_sources():
    seeds = []
    if not os.path.exists(SRC_FILE):
        return seeds
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
    seeds = read_sources()

    seen = set()
    items = []  # manifest rows

    for kind, base in seeds:
        urls = []
        if kind == "link":
            urls = [base]
        elif kind == "crawl":
            urls = crawl(base)

        for url in urls:
            name = sanitize_name(url)
            if name in seen: continue
            seen.add(name)

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

    # grava manifest
    os.makedirs(os.path.dirname(OUT_MANIFEST), exist_ok=True)
    with open(OUT_MANIFEST, "w", encoding="utf-8") as f:
        json.dump({"items": items}, f, ensure_ascii=False, indent=2)

    print(f"OK: {sum(1 for x in items if x['status']=='ok')} • indisponíveis: {sum(1 for x in items if x['status']=='unavailable')}", file=sys.stderr)

if __name__ == "__main__":
    sys.exit(main())
