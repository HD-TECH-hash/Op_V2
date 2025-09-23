#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_affix.py
- Lê scripts/affix_sources.txt (ou AFFIX_SOURCES_PATH) com linhas:
    • URL direta p/ .pdf
    • ou: 'crawl https://exemplo/pasta-ou-pagina/'  -> varre e coleta todos os .pdf daquela página
- Baixa/atualiza PDFs em data/affix/raw/
- Gera/atualiza data/affix/manifest.json com metadados (url, size, sha256, etag, last_modified)
- Substitui arquivo se o conteúdo mudou; caso contrário mantém.
- Finaliza com exit 0 mesmo sem mudanças (bom p/ GitHub Actions).

Dicas:
- Programe seu workflow para rodar diariamente às 00:00 (~00:25 é comum p/ evitar concorrência).
- Este script não faz commit; deixe o YAML commitar após a execução se houver difs.
"""

import os
import sys
import json
import time
import hashlib
import mimetypes
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

# ------------ Config ------------
ROOT = Path(__file__).resolve().parents[1]  # repo root (../..)
SOURCES_PATH = Path(os.environ.get("AFFIX_SOURCES_PATH") or ROOT / "scripts" / "affix_sources.txt")
RAW_DIR = ROOT / "data" / "affix" / "raw"
MANIFEST_PATH = ROOT / "data" / "affix" / "manifest.json"

USER_AGENT = "RIC-AFFIX-Fetcher/1.0 (+github-actions; python requests)"
TIMEOUT = (10, 30)  # connect, read
RETRIES = 2

# ------------ HTTP session with retries ------------
def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    adapter = requests.adapters.HTTPAdapter(max_retries=RETRIES)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

# ------------ Helpers ------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_filename_from_url(url: str) -> str:
    """
    Extrai o basename do path, remove query/fragment e normaliza caracteres.
    Mantém .pdf no final (se não tiver, tenta deduzir).
    """
    p = urlparse(url)
    name = unquote(os.path.basename(p.path)) or "arquivo.pdf"
    # remove coisas suspeitas
    name = name.replace("\n", " ").replace("\r", " ").strip()
    # garante extensão .pdf
    if not name.lower().endswith(".pdf"):
        # tenta deduzir pelo mimetype
        guess = mimetypes.guess_extension("application/pdf") or ".pdf"
        name += guess
    # evita nomes absurdamente longos
    if len(name) > 180:
        prefix, ext = os.path.splitext(name)
        name = prefix[:160] + ext
    return name

def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"fetched_at": None, "items": []}

def save_manifest(manifest: dict):
    manifest["items"] = sorted(manifest.get("items", []), key=lambda x: x.get("name", "").lower())
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

def normalize_pdf_url(base_url: str, href: str) -> str:
    abs_url = urljoin(base_url, href)
    # Remove anchors/fragments
    u = urlparse(abs_url)
    return u._replace(fragment="").geturl()

def is_pdf_link(href: str) -> bool:
    if not href:
        return False
    href = href.split("#", 1)[0]
    return href.lower().endswith(".pdf")

def discover_pdfs_from_page(url: str) -> set[str]:
    """Varre uma página/pasta e retorna conjunto de links absolutos .pdf"""
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"[crawl] ERRO ao abrir {url}: {e}")
        return set()

    soup = BeautifulSoup(r.text, "html.parser")
    found = set()

    # anchors
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if is_pdf_link(href):
            found.add(normalize_pdf_url(r.url, href))

    # alguns servidores listam em <pre> ou listagem; anchors já cobrem na maioria
    print(f"[crawl] {url} -> {len(found)} PDFs")
    return found

def read_sources_file(path: Path) -> list[str]:
    """Lê o arquivo de fontes e retorna lista de URLs resolvidas (sem duplicatas)."""
    if not path.exists():
        print(f"Arquivo de fontes não encontrado: {path}")
        return []
    urls: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("crawl "):
            seed = line.split(None, 1)[1].strip()
            for u in discover_pdfs_from_page(seed):
                urls.add(u)
        else:
            # URL direta
            if is_pdf_link(line):
                urls.add(line)
            else:
                # Pode haver URL sem .pdf explícito; ainda assim tente
                urls.add(line)
    print(f"[sources] Total único de URLs: {len(urls)}")
    return sorted(urls)

def head_info(url: str) -> dict:
    """Tenta HEAD para captar ETag/Last-Modified/Content-Length (opcional)."""
    info = {}
    try:
        hr = SESSION.head(url, allow_redirects=True, timeout=TIMEOUT)
        # Alguns servidores não suportam HEAD; ignore erros
        if hr.ok:
            info["etag"] = hr.headers.get("ETag")
            info["last_modified"] = hr.headers.get("Last-Modified")
            try:
                info["remote_length"] = int(hr.headers.get("Content-Length") or "0")
            except Exception:
                info["remote_length"] = None
    except Exception:
        pass
    return info

def download_pdf(url: str, dest_path: Path) -> tuple[bool, dict]:
    """
    Baixa para dest_path (substitui se conteúdo mudou).
    Retorna (changed, meta) onde:
      changed=True se arquivo foi criado/atualizado;
      meta = {name, url, size, sha256, etag, last_modified}
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Info HEAD (opcional)
    info = head_info(url)

    # Baixa para temp
    tmp = dest_path.with_suffix(dest_path.suffix + ".tmp")
    try:
        with SESSION.get(url, stream=True, timeout=TIMEOUT) as r:
            r.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
    except Exception as e:
        print(f"[download] ERRO {url}: {e}")
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        return (False, {})

    # Se já existe, compara hash
    new_hash = sha256_file(tmp)
    new_size = tmp.stat().st_size

    if dest_path.exists():
        old_hash = sha256_file(dest_path)
        if old_hash == new_hash:
            # Igual: descarta tmp
            tmp.unlink(missing_ok=True)
            meta = {
                "name": dest_path.name,
                "url": url,
                "size": dest_path.stat().st_size,
                "sha256": old_hash,
                "etag": info.get("etag"),
                "last_modified": info.get("last_modified"),
            }
            print(f"[download] SEM MUDANÇA: {dest_path.name} ({new_size} bytes)")
            return (False, meta)

    # Diferente (ou não existia): move tmp -> dest
    tmp.replace(dest_path)
    meta = {
        "name": dest_path.name,
        "url": url,
        "size": new_size,
        "sha256": new_hash,
        "etag": info.get("etag"),
        "last_modified": info.get("last_modified"),
    }
    print(f"[download] ATUALIZADO: {dest_path.name} ({new_size} bytes)")
    return (True, meta)

# ------------ Main ------------
def main() -> int:
    print("=== AFFIX FETCH ===")
    print(f"Repo root: {ROOT}")
    print(f"Sources:   {SOURCES_PATH}")
    print(f"Raw dir:   {RAW_DIR}")

    urls = read_sources_file(SOURCES_PATH)
    if not urls:
        print("Nenhuma URL encontrada nas fontes.")
        # não é erro fatal — pode ser intencional
        return 0

    manifest = load_manifest()
    existing = {item["name"]: item for item in manifest.get("items", [])}

    changed_any = False
    new_items: dict[str, dict] = {}

    for url in urls:
        fname = safe_filename_from_url(url)
        # força .pdf no nome e evita path traversal
        fname = os.path.basename(fname)
        dest = RAW_DIR / fname

        changed, meta = download_pdf(url, dest)
        if meta:
            new_items[fname] = meta
        if changed:
            changed_any = True

    # Mantém entradas antigas para arquivos que ainda existem,
    # e substitui/insere pelas novas
    final_items = {}
    # 1) começa pelas novas (fonte atual)
    final_items.update(new_items)
    # 2) mantém do manifest anterior os que ainda existem no disco e não foram sobrescritos
    for name, item in existing.items():
        path = RAW_DIR / name
        if name not in final_items and path.exists():
            final_items[name] = item

    manifest["fetched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    manifest["items"] = list(final_items.values())
    save_manifest(manifest)

    print(f"Arquivos no manifest: {len(manifest['items'])}")
    print("Concluído.")

    # Sinaliza sucesso independentemente de mudanças
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrompido.")
        sys.exit(130)
