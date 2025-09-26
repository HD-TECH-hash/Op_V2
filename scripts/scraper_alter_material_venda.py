import requests
from bs4 import BeautifulSoup
import csv
import os

BASE_URL = "https://www.alter.com.br/portal-do-parceiro/material-de-venda/"
OUTPUT_DIR = "data/affix/raw"
CSV_PATH = "data/affix/alter_pdfs_manifest.csv"

def fetch_alter_pdfs():
    print("🔎 Acessando página...")
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, "html.parser")

    pdf_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".pdf"):
            text = a.get_text(strip=True)
            name = text if text else os.path.basename(href)
            pdf_links.append((name, href))

    print(f"✅ Encontrados {len(pdf_links)} PDFs")

    # salvar CSV
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "url"])
        writer.writerows(pdf_links)

    # baixar PDFs no RAW
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for name, url in pdf_links:
        filename = os.path.join(OUTPUT_DIR, os.path.basename(url))
        try:
            resp = requests.get(url)
            with open(filename, "wb") as f:
                f.write(resp.content)
            print(f"⬇️ {filename}")
        except Exception as e:
            print(f"⚠️ Erro ao baixar {url}: {e}")

if __name__ == "__main__":
    fetch_alter_pdfs()
