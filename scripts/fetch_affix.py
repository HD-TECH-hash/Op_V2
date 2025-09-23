#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Baixa PDFs da Affix a partir de scripts/affix_sources.txt.

• Linhas aceitas no sources:
    link <URL-PDF>
    crawl <URL-PAGINA-OU-PASTA>
  - Se a linha for "crawl <URL-que-termina-em-.pdf>", tratamos como link direto.

• Filtro 2025:
    - Se a env AFFIX_ONLY_2025=1, mantém apenas URLs que:
        - tenham "/2025/" no caminho OU
        - cujo nome do arquivo termine com "-25.pdf" (ex.: ...-01-25.pdf)
    - Caso contrário, baixa todos.

• Salvamento:
    - Destino: data/affix/raw/<nome-arquivo>.pdf
    - Sobrescreve se já existir (mesmo nome).
    - Gera/atualiza índice: data/affix/raw/index.json

Dependências:
    pip install requests beautifulsoup4
"""

from __future__ import annotations
import os, re, sys, json, time, hashlib, logging
from urllib.parse import urljoin, urlparse, unquote
from typing import Iterable, List, Tuple, Set

# --- Configs básicas
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_DIR  = os.path.join(ROOT_DIR, "data", "affix", "raw")
SRC_FILE = os.path.join(ROOT_DIR, "scripts", "affix_sources.txt")
INDEX    = os.path.join(OUT_DIR, "index.json")

ONLY_2025 = os.environ.get("AFFIX_ONLY_2025", "0").strip() in ("1", "true", "TRUE", "yes", "on")

# --- Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_affix")

# --- HTTP
try:
    import requests
    REQ_OK = True
except Exception as e:
    print("ERRO: requests não disponível. Instale com 'pip install requests'.", file=sys.stderr)
    REQ_OK = False

try:
    from bs4 import BeautifulSoup  # type: ignore
    HAVE_BS4 = True
except Exception:
    HAVE_BS4 = False

SESSION = requests.Session() if REQ_OK else None
SESSION.headers.update({
    "User-Agent": "RicAI-AffixFetcher/1.0 (+github actions)"
}) if REQ_OK else None
TIMEOUT = 30


# ----------------- Utils -----------------
def ensure_dirs() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

def read_sources(path: str) -> List[Tuple[str, str]]:
    """
    Retorna lista [(cmd, url)], onde cmd ∈ {"link","crawl"}.
    Ignora linhas vazias e comentários (#).
    """
    out: List[Tuple[str,str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"): 
                continue
            m = re.match(r"^(link|crawl)\s+(\S+)$", line, flags=re.IGNORECASE)
            if not m:
                log.warning("Linha ignorada (formato inválido): %s", line)
                continue
            cmd, url = m.group(1).lower(), m.group(2)
            out.append((cmd, url))
    return out

def is_pdf_url(url: str) -> bool:
    return ".pdf" in url.lower().split("?")[0]

def looks_2025(url: str) -> bool:
    if "/2025/" in url:
        return True
    name = os.path.basename(urlparse(url).path)
    return bool(re.search(r"-25\.pdf$", name, flags=re.IGNORECASE))

def normalize_filename_from_url(url: str) -> str:
    name = os.path.basename(urlparse(url).path)
    name = unquote(name)
    # remove fragment/query-artefacts
    name = name.split("?")[0].split("#")[0]
    # segurança mínima
    name = name.replace("/", "_").replace("\\", "_")
    return name

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_index() -> dict:
    if not os.path.exists(INDEX):
        return {"items": []}
    try:
        with open(INDEX, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"items": []}

def save_index(idx: dict) -> None:
    tmp = INDEX + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)
    os.replace(tmp, INDEX)

def add_or_update_index(idx: dict, name: str, url: str, size: int, sha256: str) -> None:
    items = idx.setdefault("items", [])
    for it in items:
        if it.get("name") == name:
            it.update({
                "url": url, "size": size, "sha256": sha256,
                "updated": int(time.time()),
            })
            return
    items.append({
        "name": name, "url": url, "size": size, "sha256": sha256,
        "updated": int(time.time()),
    })

# ----------------- Download -----------------
def get(url: str) -> requests.Response:
    return SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)

def head(url: str) -> requests.Response:
    try:
        return SESSION.head(url, timeout=TIMEOUT, allow_redirects=True)
    except Exception:
        # alguns servidores não suportam HEAD
        return SESSION.get(url, stream=True, timeout=TIMEOUT, allow_redirects=True)

def download_pdf(url: str, out_path: str) -> Tuple[bool, str]:
    """
    Baixa PDF em out_path (sobrescreve). Retorna (ok, motivo_erro_ou_vazio).
    """
    tries = 3
    last_err = ""
    for i in range(1, tries+1):
        try:
            r = SESSION.get(url, stream=True, timeout=TIMEOUT)
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                time.sleep(1.2*i); continue

            # deteção básica de PDF
            head_bytes = r.raw.read(5)
            if head_bytes != b"%PDF-"[:len(head_bytes)]:
                # Alguns servidores só começam depois do primeiro chunk.
                # Se não for claramente PDF, ainda gravamos e checamos depois.
                pass

            tmp = out_path + ".part"
            with open(tmp, "wb") as f:
                f.write(head_bytes)
                for chunk in r.iter_content(1024*256):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp, out_path)
            return True, ""
        except Exception as e:
            last_err = str(e)
            time.sleep(1.2*i)
    return False, last_err

# ----------------- Crawl -----------------
def extract_pdf_links_from_page(url: str) -> List[str]:
    try:
        r = get(url)
    except Exception as e:
        log.error("Falha ao abrir página: %s (%s)", url, e)
        return []
    if r.status_code != 200:
        log.error("Falha HTTP %s em %s", r.status_code, url)
        return []

    html = r.text
    out: Set[str] = set()
    if HAVE_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            absu = urljoin(url, href)
            if is_pdf_url(absu):
                out.add(absu)
    else:
        # fallback simples por regex
        for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
            href = m.group(1)
            absu = urljoin(url, href)
            if is_pdf_url(absu):
                out.add(absu)
    return sorted(out)

# ----------------- Main -----------------
def main() -> int:
    if not REQ_OK:
        return 1
    ensure_dirs()

    sources = read_sources(SRC_FILE)
    if not sources:
        log.error("Nenhuma fonte em %s", SRC_FILE)
        return 1

    # Coleta de URLs
    urls: List[str] = []
    seen: Set[str] = set()
    for cmd, url in sources:
        if cmd == "link":
            if url not in seen:
                urls.append(url); seen.add(url)
            continue

        # cmd == "crawl"
        if is_pdf_url(url):
            # 'crawl' apontando para .pdf -> tratar como link
            if url not in seen:
                urls.append(url); seen.add(url)
            continue

        # crawl página/pasta
        found = extract_pdf_links_from_page(url)
        for u in found:
            if u not in seen:
                urls.append(u); seen.add(u)

    # Filtro 2025 (se habilitado)
    if ONLY_2025:
        before = len(urls)
        urls = [u for u in urls if looks_2025(u)]
        log.info("Filtro 2025 ativo: %d -> %d URLs elegíveis", before, len(urls))

    if not urls:
        log.error("Nenhum PDF elegível após parsing/filtros.")
        return 1

    # Download
    idx = load_index()
    ok_count = 0
    fail_count = 0

    for url in urls:
        name = normalize_filename_from_url(url)
        if not name.lower().endswith(".pdf"):
            name += ".pdf"
        dest = os.path.join(OUT_DIR, name)

        log.info("Baixando: %s -> %s", url, name)
        ok, err = download_pdf(url, dest)
        if not ok:
            log.error("Falhou: %s (%s)", name, err)
            fail_count += 1
            continue

        size = os.path.getsize(dest)
        digest = sha256_file(dest)
        add_or_update_index(idx, name=name, url=url, size=size, sha256=digest)
        ok_count += 1

    save_index(idx)
    log.info("Concluído. OK=%d, Falhou=%d", ok_count, fail_count)

    # sucesso se ao menos 1 arquivo baixado/atualizado
    return 0 if ok_count > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
