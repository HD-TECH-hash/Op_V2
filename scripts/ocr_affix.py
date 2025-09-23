#!/usr/bin/env python3
# scripts/ocr_affix.py
from pathlib import Path
import subprocess, re

RAW = Path("data/affix/raw")
OCR = Path("data/affix/ocr")
OCR.mkdir(parents=True, exist_ok=True)

def safe_dir(stem: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._\-]+", "_", stem.strip()) or "pdf"
    return OCR / safe

def run(cmd):
    try:
        print("$", " ".join(map(str, cmd)))
        subprocess.run(cmd, check=True)
        return True
    except Exception as e:
        print("cmd fail:", e)
        return False

def ensure_thumb(pdf: Path, outdir: Path) -> Path | None:
    outdir.mkdir(parents=True, exist_ok=True)
    single = outdir / "page1"
    thumb  = outdir / "thumb.png"
    if thumb.exists(): 
        return thumb
    ok = run(["pdftoppm", "-f", "1", "-l", "1", "-png", "-singlefile", "-scale-to", "1280", str(pdf), str(single)])
    if ok and (single.with_suffix(".png")).exists():
        (single.with_suffix(".png")).rename(thumb)
        return thumb
    return None

def ensure_text(img: Path, outdir: Path) -> Path | None:
    txt = outdir / "text.txt"
    if txt.exists(): 
        return txt
    # tesseract gera <base>.txt a partir do <base> sem extensão:
    outbase = outdir / "text"
    ok = run(["tesseract", str(img), str(outbase), "-l", "por+eng", "--psm", "6"])
    gen = outbase.with_suffix(".txt")
    if ok and gen.exists():
        if gen != txt:
            gen.replace(txt)
        return txt
    return None

def main():
    if not RAW.exists():
        print("Nada em", RAW)
        return

    for pdf in sorted(RAW.glob("*.pdf")):
        outdir = safe_dir(pdf.stem)
        thumb = ensure_thumb(pdf, outdir)
        if thumb:
            ensure_text(thumb, outdir)

    print("OCR concluído.")

if __name__ == "__main__":
    main()
