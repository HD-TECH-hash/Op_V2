#!/usr/bin/env python3
# scripts/fetch_affix.py
from pathlib import Path
from urllib.parse import urlsplit, unquote
import json, time
import requests

RAW_DIR = Path("data/affix/raw")
MANIFEST = Path("data/affix/manifest.json")
SOURCES = Path("scripts/affix_sources.txt")

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

def iter_sources():
    if not SOURCES.exists():
        print(f"[ERRO] Arquivo de fontes não encontrado: {SOURCES}")
        return
    for line in SOURCES.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        yield line

def filename_from_url(url: str) -> str:
    path = unquote(urlsplit(url).path)
    name = Path(path).name or "arquivo.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def head(url, headers):
    try:
        return requests.head(url, headers=headers, allow_redirects=True, timeout=30)
    except Exception:
        return None

def download(url, dest: Path, cond_headers):
    with requests.get(url, headers={**cond_headers, "User-Agent": UA}, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)
        return r.headers

def main():
    manifest = load_manifest()
    changed = 0
    for url in iter_sources():
        name = filename_from_url(url)
        dest = RAW_DIR / name
        rec = manifest.get(url, {})
        cond = {"User-Agent": UA}
        if rec.get("etag"): cond["If-None-Match"] = rec["etag"]
        if rec.get("last_modified"): cond["If-Modified-Since"] = rec["last_modified"]

        print(f"\n⇒ {name}")
        h = head(url, {"User-Agent": UA,
                       **({"If-None-Match": rec.get("etag")} if rec.get("etag") else {}),
                       **({"If-Modified-Since": rec.get("last_modified")} if rec.get("last_modified") else {})})
        if h is not None and h.status_code == 304:
            print("   304 Not Modified — pulando download.")
            continue

        try:
            headers = download(url, dest, cond)
            etag = headers.get("ETag")
            last_mod = headers.get("Last-Modified")
            manifest[url] = {
                "file": str(dest),
                "etag": etag,
                "last_modified": last_mod,
                "updated_at": int(time.time())
            }
            changed += 1
            print(f"   OK → {dest}")
        except requests.HTTPError as e:
            print(f"   HTTP erro: {e} ({e.response.status_code})")
        except Exception as e:
            print(f"   Falhou: {e}")

    save_manifest(manifest)
    print(f"\nConcluído. Arquivos novos/atualizados: {changed}")

if __name__ == "__main__":
    main()
