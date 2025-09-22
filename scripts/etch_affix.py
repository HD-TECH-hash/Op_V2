#!/usr/bin/env python3
# scripts/fetch_affix.py
from pathlib import Path
from urllib.parse import urlsplit, unquote
import hashlib, json, time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

RAW_DIR     = Path("data/affix/raw")
MANIFEST    = Path("data/affix/manifest.json")
SOURCES_TXT = Path("scripts/affix_sources.txt")

UA = "Mozilla/5.0 (compatible; RicCrionBot/1.0; +https://github.com/HD-TECH-hash)"

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

def norm_filename_from_url(url: str) -> str:
    p = urlsplit(url)
    name = Path(unquote(p.path)).name or "arquivo.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    # remove trechos perigosos
    name = name.replace("/", "_").replace("\\", "_").replace("?", "_")
    return name

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def is_pdf_url(url: str) -> bool:
    up = urlsplit(url)
    return up.path.lower().endswith(".pdf")

def list_pdf_links_from_page(url: str) -> list[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().endswith(".pdf"):
                # resolver links relativos
                pdf_url = requests.compat.urljoin(url, href)
                pdfs.append(pdf_url)
        return sorted(set(pdfs))
    except Exception as e:
        print(f"[WARN] Falha ao varrer página: {url} -> {e}")
        return []

def read_sources() -> list[str]:
    if not SOURCES_TXT.exists():
        print(f"[ERRO] {SOURCES_TXT} não encontrado.")
        return []
    urls = []
    for line in SOURCES_TXT.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls

def download_pdf(url: str, manifest: dict) -> None:
    fname = norm_filename_from_url(url)
    dest  = RAW_DIR / fname

    try:
        with requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=60) as r:
            r.raise_for_status()
            tmp = dest.with_suffix(".downloading")
            total = int(r.headers.get("Content-Length") or 0)
            pbar = tqdm(total=total, unit="B", unit_scale=True, desc=fname)
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024*64):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))
            pbar.close()

        new_hash = sha256_file(tmp)
        old_hash = manifest.get(fname, {}).get("sha256")

        # somente substitui se mudou ou não existia
        if dest.exists() and old_hash == new_hash:
            tmp.unlink(missing_ok=True)
            print(f"[SKIP] Sem mudanças: {fname}")
        else:
            tmp.replace(dest)
            manifest[fname] = {
                "url": url,
                "sha256": new_hash,
                "ts": int(time.time())
            }
            print(f"[OK] Salvo: {dest}")

    except requests.HTTPError as e:
        print(f"[HTTP] {e.response.status_code} ao baixar {url}")
    except Exception as e:
        print(f"[ERRO] Falha ao baixar {url} -> {e}")

def main():
    manifest = load_manifest()
    seeds = read_sources()
    if not seeds:
        return

    # expande: links diretos + páginas com PDFs
    all_pdfs = []
    for s in seeds:
        if is_pdf_url(s):
            all_pdfs.append(s)
        else:
            all_pdfs.extend(list_pdf_links_from_page(s))

    # de-duplicar preservando ordem
    seen = set()
    queue = []
    for u in all_pdfs:
        if u not in seen:
            seen.add(u)
            queue.append(u)

    print(f"Encontrados {len(queue)} PDFs para processar.")

    for url in queue:
        download_pdf(url, manifest)

    save_manifest(manifest)
    print("Concluído.")

if __name__ == "__main__":
    main()
