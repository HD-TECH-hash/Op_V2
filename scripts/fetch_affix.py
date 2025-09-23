#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baixa PDFs da Affix a partir de seeds em scripts/affix_sources.txt.
Suporta:
  - Linha com URL direta para PDF
  - Linha "crawl <URL>" para varrer a página e coletar todos os .pdf visíveis
Gera/atualiza:
  - data/affix/raw/<arquivo>.pdf
  - data/affix/manifest.json  (com link oficial, size, sha256, updated)
"""

from __future__ import annotations
import os, re, json, hashlib, time
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
SRC_FILE = ROOT / "scripts" / "affix_sources.txt"
RAW_DIR  = ROOT / "data" / "affix" / "raw"
DATA_DIR = ROOT / "data" / "affix"
MANIFEST = DATA_DIR / "manifest.json"

RAW_DIR.mkdir(parents=True, exist_ok=True)

UA = {"User-Agent": "ricai-affix-fetcher (+github actions)"}

def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_name_from_url(url: str) -> str:
    # usa último segmento do path, decodifica %20 etc.
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    # fallback
    if not name or not name.lower().endswith(".pdf"):
        name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name or "file.pdf")
        if not name.lower().endswith(".pdf"):
            name += ".pdf"
    return name

def head(url: str):
    try:
        r = requests.head(url, timeout=25, allow_redirects=True, headers=UA)
        if r.ok:
            return r.headers
    except Exception:
        return None
    return None

def download(url: str, dest: Path) -> tuple[bool, int]:
    """Baixa para dest; retorna (baixou?, bytes)."""
    with requests.get(url, stream=True, timeout=60, headers=UA) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        total = 0
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 128):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        tmp.replace(dest)
        return True, total

def discover_pdfs_from_page(page_url: str) -> list[str]:
    urls = set()
    try:
        r = requests.get(page_url, timeout=40, headers=UA)
        r.raise_for_status()
    except Exception as e:
        print(f"[WARN] falha ao abrir {page_url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(page_url, href)
        if re.search(r"\.pdf(\?.*)?$", full, re.I):
            urls.add(full.split("?")[0])  # normaliza, remove query
    # alguns diretórios WordPress mostram a listagem simples:
    # se a página for um índice "cru", ainda assim os <a> aparecem.
    return sorted(urls)

def read_sources() -> list[str]:
    lines = []
    if not SRC_FILE.exists():
        print(f"[ERROR] {SRC_FILE} não existe.")
        return lines
    for raw in SRC_FILE.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s)
    return lines

def load_manifest() -> dict:
    if MANIFEST.exists():
        try:
            return json.loads(MANIFEST.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"generated_at": 0, "items": []}

def save_manifest(items: list[dict]):
    MANIFEST.write_text(
        json.dumps({"generated_at": int(time.time()), "items": items}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def main():
    sources = read_sources()
    manifest = load_manifest()
    by_name = {it.get("name"): it for it in manifest.get("items", [])}
    seen_names = set()

    discovered = []  # (url_pdf, referer)
    for s in sources:
        if s.lower().startswith("crawl "):
            page = s.split(" ", 1)[1].strip()
            print(f"[crawl] {page}")
            for pdf in discover_pdfs_from_page(page):
                discovered.append((pdf, page))
        elif re.search(r"^https?://.+\.pdf(\?.*)?$", s, re.I):
            discovered.append((s, None))
        else:
            print(f"[skip] {s}")

    total_new = total_kept = 0
    items_out = []

    for url_pdf, referer in discovered:
        url_pdf = url_pdf.split("?")[0]
        name = safe_name_from_url(url_pdf)
        dest = RAW_DIR / name
        seen_names.add(name)

        # HEAD/ETag/Length para decidir se baixa de novo
        remote_len = None
        hdr = head(url_pdf)
        if hdr:
            try:
                remote_len = int(hdr.get("Content-Length") or 0)
            except Exception:
                remote_len = None

        need = True
        old_sha = None
        if dest.exists():
            if remote_len is not None and dest.stat().st_size == remote_len:
                need = False  # mesma length: assume igual
            else:
                old_sha = sha256_of(dest)

        if need:
            try:
                print(f"[get] {name}")
                _, _ = download(url_pdf, dest)
                total_new += 1
            except Exception as e:
                print(f"[ERR] {url_pdf}: {e}")
                continue

            if old_sha and sha256_of(dest) == old_sha:
                # conteúdo igual apesar de tamanho diferente/HEAD falho
                pass
        else:
            total_kept += 1

        # atualiza manifest
        item = by_name.get(name, {}).copy()
        item.update({
            "name": name,
            "url": url_pdf,                   # link oficial (affix.com.br)
            "size": dest.stat().st_size,
            "sha256": sha256_of(dest),
            "updated": int(time.time()),
            "referer": referer or url_pdf,
        })
        items_out.append(item)

    # mantém apenas os arquivos "vistos" nesta rodada (ficam na pasta; se upstream mudar amanhã, a gente substitui)
    items_out.sort(key=lambda x: x["name"].lower())
    save_manifest(items_out)

    print(f"[done] novos: {total_new} • mantidos: {total_kept} • total: {len(items_out)}")
    print(f"[hint] manifest em {MANIFEST.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
