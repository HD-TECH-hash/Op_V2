# --- troque a função por esta ---
YEARS = {"2025","2024"}  # adicione mais se quiser

def is_pdf_accepted(url: str) -> bool:
    u = url.split("#")[0].split("?")[0]
    if not u.lower().endswith(".pdf"):
        return False
    try:
        netloc = urlparse(u).netloc.lower()
        if not netloc.endswith("affix.com.br"):
            return False
    except Exception:
        return False
    # aceita se a URL tiver /YYYY/ para qualquer ano em YEARS
    for y in YEARS:
        if f"/{y}/" in u or u.lower().endswith(f"-{y[-2:]}.pdf"):
            return True
    # Se quiser SEM filtro de ano, basta "return True" aqui.
    return False
