# scripts/fetch_affix.py
from pathlib import Path
from urllib.parse import urlsplit, unquote
import requests, json, time, sys, hashlib

RAW_DIR   = Path("data/affix/raw")
MANIFEST  = Path("data/affix/manifest.json")
SOURCES   = Path("scripts/affix_sources.txt")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

def load_manifest():
    try:
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_manifest(m):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")

def sanitize_name(url: str) -> str:
    name = Path(unquote(urlsplit(url).path)).name or "file.pdf"
    if not name.lower().endswith(".pdf"):
        name = hashlib.md5(url.encode()).hexdigest() + ".pdf"
    return name

def fetch_one(url: str) -> bool:
    name = sanitize_name(url)
    dst  = RAW_DIR / name
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"→ Baixando: {url}")
    try:
        headers = {
            "User-Agent": UA,
            "Referer": "https://www.affix.com.br/",
            "Accept": "*/*",
        }
        r = requests.get(url, headers=headers, allow_redirects=True, timeout=60, stream=True)
        print(f"  status={r.status_code}  type={r.headers.get('Content-Type')}  len={r.headers.get('Content-Length')}")
        if r.status_code != 200:
            print(f"  ERRO: HTTP {r.status_code}")
            return False

        tmp = dst.with_suffix(dst.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        tmp.rename(dst)
        print(f"  ✓ Salvo em {dst}")
        return True
    except Exception as e:
        print("  EXCEÇÃO:", repr(e))
        return False

def main():
    if not SOURCES.exists():
        print(f"Arquivo de fontes não encontrado: {SOURCES}")
        sys.exit(1)

    urls = [
        ln.strip() for ln in SOURCES.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    print("Fontes:", urls)
    if not urls:
        print("Nenhum URL na lista.")
        sys.exit(0)

    ok = 0
    manifest = load_manifest()
    for url in urls:
        if fetch_one(url):
            ok += 1
            manifest[url] = {"ts": int(time.time())}
    save_manifest(manifest)
    print(f"Concluído. Baixados: {ok}/{len(urls)}")
    # exit 0 mesmo que 0 arquivos, para o job não falhar à toa
    sys.exit(0)

if __name__ == "__main__":
    main()
