#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib, os, re, sys, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ROOT = os.path.dirname(os.path.dirname(__file__))  # repo root
SOURCES = os.path.join(ROOT, "scripts", "affix_sources.txt")
OUTDIR  = os.path.join(ROOT, "data", "affix", "raw")
os.makedirs(OUTDIR, exist_ok=True)

UA = {"User-Agent": "RicAI-AffixFetcher/1.0 (+github actions)"}
PDF_RE = re.compile(r"\.pdf(\?.*)?$", re.I)

def sha256_bytes(b): return hashlib.sha256(b).hexdigest()
def sanitize_name(name):
    name = name.strip().replace("\u00a0", " ")
    name = re.sub(r"[^\w\-.()+@\s]", "-", name)
    name = re.sub(r"\s+", "-", name)
    return name

def save_bytes(path, data):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f: f.write(data)
    os.replace(tmp, path)

def fetch_link(url):
    # baixa PDF exato
    r = requests.get(url, headers=UA, timeout=60, stream=True)
    r.raise_for_status()
    # nome pelo path
    name = sanitize_name(os.path.basename(urlparse(url).path)) or f"file-{int(time.time())}.pdf"
    if not name.lower().endswith(".pdf"): name += ".pdf"
    out = os.path.join(OUTDIR, name)

    new = b"".join(r.iter_content(1024 * 64))
    new_hash = sha256_bytes(new)
    if os.path.exists(out):
        with open(out, "rb") as f: old_hash = sha256_bytes(f.read())
        if old_hash == new_hash:
            print(f"[=] {name} (sem mudanças)")
            return 0
    save_bytes(out, new)
    print(f"[+] {name} ({len(new)} bytes)")
    return 1

def crawl_page(seed):
    print(f"[*] crawl {seed}")
    r = requests.get(seed, headers=UA, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    found = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not PDF_RE.search(href): continue
        url = urljoin(seed, href)
        found.append(url)
    # dedupe mantendo ordem
    seen, urls = set(), []
    for u in found:
        if u not in seen:
            seen.add(u); urls.append(u)
    print(f"    -> {len(urls)} pdf(s)")
    ok = 0
    for url in urls:
        try: ok += fetch_link(url)
        except Exception as e: print(f"[!] falha {url}: {e}")
        time.sleep(0.5)
    return ok

def main():
    if not os.path.exists(SOURCES):
        print(f"arquivo não encontrado: {SOURCES}", file=sys.stderr)
        sys.exit(1)

    total_new = 0
    with open(SOURCES, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

    for ln in lines:
        if ln.lower().startswith("link "):
            url = ln.split(None, 1)[1].strip()
            try: total_new += fetch_link(url)
            except Exception as e: print(f"[!] link falhou {url}: {e}")
        elif ln.lower().startswith("crawl "):
            url = ln.split(None, 1)[1].strip()
            try: total_new += crawl_page(url)
            except Exception as e: print(f"[!] crawl falhou {url}: {e}")
        else:
            print(f"[?] linha ignorada: {ln}")

    print(f"\nFeito. Arquivos novos/atualizados: {total_new}")

if __name__ == "__main__":
    main()
