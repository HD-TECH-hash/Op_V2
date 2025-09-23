#!/usr/bin/env python3
# scripts/fetch_affix.py
import os, re, sys, json, hashlib, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = os.path.dirname(os.path.dirname(__file__))   # repo root
RAW_DIR = os.path.join(ROOT, "data", "affix", "raw")
OUT_MANIFEST = os.path.join(ROOT, "data", "affix", "manifest.json")
SRC_FILE = os.path.join(ROOT, "scripts", "affix_sources.txt")

HEADERS = {"User-Agent": "RICAI-AFFIX/1.0 (+github actions)"}
TIMEOUT = 40

ONLY_2025 = os.getenv("AFFIX_ONLY_2025", "0").strip() not in ("", "0", "false", "False", "no")

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_MANIFEST), exist_ok=True)

def is_pdf_url(url: str) -> bool:
    return url.lower().endswith(".pdf")

def allow_2025(url: str) -> bool:
    """Se ligado, aceita apenas:
       - caminho com '/2025/' (pasta do WP)
       - ou nome terminando em '-MM-25.pdf' (ex.: _10-25.pdf)
       - ou '01-25.pdf' etc (alguns estão em pastas antigas)"""
    if not ONLY_2025:
        return True
    if "/2025/" in url:
        return True
    if re.search(r"[-_/](0[1-9]|1[0-2])-25\.pdf$", url, re.IGNORECASE):
        return True
    return False

def sanitize_name(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or ("file-"+hashlib.md5(url.encode()).hexdigest()+".pdf")
    name = re.sub(r"[^A-Za-z0-9._\-]+", "_", name)
    return name[:180]

def head_ok(url: str) -> bool:
    try:
        r = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=TIMEOUT)
        ct = (r.headers.get("Content-Type","") or "").lower()
        return (r.status_code == 200) and ("pdf" in ct or url.lower().endswith(".pdf"))
    except Exception:
        return False

def fetch_pdf(url: str, dest: str) -> int:
    with requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1024*64):
                if chunk:
                    f.write(chunk)
    return os.path.getsize(dest)

def crawl(url: str) -> list[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            absu = urljoin(url, a["href"])
            if is_pdf_url(absu):
                links.append(absu)
        # remove duplicados mas mantém ordem de aparição
        return list(dict.fromkeys(links))
    except Exception:
        return []

def read_sources() -> list[tuple[str, str]]:
    seeds: list[tuple[str, str]] = []
    if not os.path.exists(SRC_FILE):
        return seeds
    with open(SRC_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^(link|crawl)\s+(https?://\S+)$", line, flags=re.I)
            if m:
                seeds.append((m.group(1).lower(), m.group(2)))
    return seeds

def main() -> int:
    ensure_dirs()
    seeds = read_sources()

    seen_names = set()
    items = []   # linhas do manifest

    for kind, base in seeds:
        urls = [base] if kind == "link" else crawl(base)

        for url in urls:
            if not is_pdf_url(url):
                continue
            if not allow_2025(url):
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
                    print(f"[ok] {name}  {size} bytes")
                except Exception as e:
                    row["status"] = f"error: {e.__class__.__name__}"
                    print(f"[erro] {name}: {e}", file=sys.stderr)
            else:
                row["status"] = "unavailable"
                print(f"[404?] {name} — indisponível", file=sys.stderr)

            items.append(row)

    with open(OUT_MANIFEST, "w", encoding="utf-8") as f:
        json.dump({"items": items, "generated_at": int(time.time())}, f, ensure_ascii=False, indent=2)

    ok = sum(1 for x in items if x["status"] == "ok")
    un = sum(1 for x in items if x["status"] == "unavailable")
    print(f"OK: {ok} • indisponíveis: {un}", file=sys.stderr)
    return 0

if __name__ == "__main__":
    sys.exit(main())
