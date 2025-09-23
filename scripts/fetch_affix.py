#!/usr/bin/env python3
# scripts/fetch_affix.py
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote
import requests, hashlib, json, time, re
from bs4 import BeautifulSoup

RAW_DIR = Path("data/affix/raw")
MANIFEST = Path("data/affix/manifest.json")
SOURCES  = Path("scripts/affix_sources.txt")

UA = "Mozilla/5.0 (compatible; RicCrionBot/1.0; https://github.com/HD-TECH-hash)"

RAW_DIR.mkdir(parents=True, exist_ok=True)

def load_manifest():
    if MANIFEST.exists():
        try:
            return json.loads(MANIFEST.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_manifest(m):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def http_head(url):
    try:
        r = requests.head(url, timeout=30, headers={"User-Agent": UA}, allow_redirects=True)
        if r.status_code >= 400:
            return None
        return r.headers
    except Exception:
        return None

def http_get(url):
    r = requests.get(url, timeout=120, headers={"User-Agent": UA}, stream=True)
    r.raise_for_status()
    return r

def safe_name_from_url(url):
    path = unquote(urlparse(url).path)
    name = Path(path).name or "arquivo.pdf"
    # normaliza qualquer esquisitice
    name = re.sub(r"[^A-Za-z0-9._\-]+", "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def crawl_pdfs(url):
    """Varre uma página e retorna links absolutos para .pdf"""
    urls = []
    try:
        r = requests.get(url, timeout=60, headers={"User-Agent": UA})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a["href"].strip()
            if not href: continue
            absu = urljoin(url, href)
            if ".pdf" in absu.lower():
                urls.append(absu.split("#")[0])
    except Exception:
        pass
    # dedupe preservando ordem
    seen = set(); out = []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def read_sources():
    direct = []
    crawl  = []
    if not SOURCES.exists():
        return direct, crawl
    for raw in SOURCES.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("crawl "):
            crawl.append(line.split(" ",1)[1].strip())
        else:
            direct.append(line)
    return direct, crawl

def download_if_needed(url, manifest):
    name = safe_name_from_url(url)
    dest = RAW_DIR / name

    # verifica por tamanho no HEAD
    h = http_head(url)
    remote_size = None
    if h and "content-length" in (k:= {k.lower():v for k,v in h.items()}):
        try:
            remote_size = int(k["content-length"])
        except Exception:
            remote_size = None

    if dest.exists() and remote_size and dest.stat().st_size == remote_size:
        # já temos com o mesmo tamanho — pula
        return name, False

    # baixa
    r = http_get(url)
    with dest.open("wb") as f:
        for chunk in r.iter_content(1024*128):
            if chunk: f.write(chunk)

    # atualiza manifest
    entry = {
        "name": name,
        "url": url,
        "size": dest.stat().st_size,
        "sha256": sha256_file(dest),
        "updated": int(time.time()),
    }
    manifest[name] = entry
    return name, True

def main():
    manifest = load_manifest()

    direct, crawl = read_sources()
    urls = list(direct)

    for root in crawl:
        urls.extend(crawl_pdfs(root))

    # dedupe preservando ordem
    seen = set(); final = []
    for u in urls:
        if u not in seen:
            seen.add(u); final.append(u)

    changed = 0
    for url in final:
        try:
            _, did = download_if_needed(url, manifest)
            if did: changed += 1
        except Exception as e:
            print("Erro ao baixar:", url, "->", e)

    save_manifest(manifest)
    print(f"Concluído. PDFs totais: {len(final)} • atualizados/novos: {changed}")

if __name__ == "__main__":
    main()
