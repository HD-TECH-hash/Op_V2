#!/usr/bin/env python3
# scripts/fetch_affix.py
from pathlib import Path
from urllib.parse import urlsplit, unquote
import hashlib, json, time, re
import requests

RAW_DIR     = Path("data/affix/raw")
MANIFEST    = Path("data/affix/manifest.json")
SOURCES_TXT = Path("scripts/affix_sources.txt")

UA = "Mozilla/5.0 (compatible; RicCrionBot/1.0; https://github.com/HD-TECH-hash)"

def slugify(name: str) -> str:
    # mantém extensão, saneia o resto
    name = unquote(name.strip())
    name = name.replace(" ", "-")
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name)
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name or "arquivo.pdf"

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_sources():
    if not SOURCES_TXT.exists():
        print("⚠️  scripts/affix_sources.txt não encontrado.")
        return []
    out = []
    for line in SOURCES_TXT.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out

def load_manifest():
    if MANIFEST.exists():
        try:
            return json.loads(MANIFEST.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"generated_at": None, "items": {}}

def save_manifest(m):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    m["generated_at"] = int(time.time())
    MANIFEST.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"📝 Manifesto salvo em {MANIFEST}")

def download_file(url: str, dst: Path) -> dict:
    headers = {"User-Agent": UA}
    with requests.get(url, headers=headers, stream=True, timeout=90) as r:
        r.raise_for_status()
        tmp = dst.with_suffix(dst.suffix + ".part")
        dst.parent.mkdir(parents=True, exist_ok=True)
        bytes_written = 0
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
                    bytes_written += len(chunk)
        tmp.replace(dst)
    digest = sha256_file(dst)
    print(f"✅ Baixado: {dst.name} • {bytes_written/1_000_000:.2f} MB • sha256={digest[:12]}…")
    return {
        "name": dst.name,
        "local_path": str(dst.as_posix()),
        "bytes": bytes_written,
        "sha256": digest,
        "url": url,
        "updated": int(time.time()),
    }

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    sources = read_sources()
    if not sources:
        print("⚠️  Nenhum link em scripts/affix_sources.txt")
        return

    manifest = load_manifest()
    items = manifest.get("items") or {}

    for url in sources:
        try:
            path = urlsplit(url).path
            base = path.rsplit("/", 1)[-1] if "/" in path else path
            name = slugify(base or "arquivo.pdf")
            dst  = RAW_DIR / name

            # baixa sempre que não existir ou se falhar a leitura do sha
            need = not dst.exists()
            if not need:
                try:
                    _ = sha256_file(dst)
                except Exception:
                    need = True

            if need:
                meta = download_file(url, dst)
                items[name] = meta
            else:
                # já existe: atualiza metadados mínimos
                meta = items.get(name) or {}
                meta.update({
                    "name": name,
                    "local_path": str(dst.as_posix()),
                    "bytes": dst.stat().st_size,
                    "sha256": sha256_file(dst),
                    "url": url,
                    "updated": int(time.time()),
                })
                items[name] = meta
                print(f"↩️  Mantido (já existia): {name}")

        except Exception as e:
            print(f"❌ Erro ao processar {url}: {e}")

    manifest["items"] = items
    save_manifest(manifest)

if __name__ == "__main__":
    main()
