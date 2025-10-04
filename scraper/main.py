# scraper/main.py
from __future__ import annotations
import os, re, json, time, sys, pathlib, datetime as dt
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import requests
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

CONFIG_PATH = ROOT / "config.json"
DEFAULT_CONFIG = {
    "base_url": "",
    "pages": 3,
    "sleep_seconds": 1.0,
    "user_agent": "Mozilla/5.0 (compatible; VehiculosScraper/1.1)",
    "details": True,
    "detail_sleep_seconds": 0.8,
    "max_details": 120
}

def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return {**DEFAULT_CONFIG, **json.load(f)}
    return DEFAULT_CONFIG

def add_or_replace_query(url: str, **params):
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q.update({k: str(v) for k,v in params.items()})
    new = parsed._replace(query=urlencode(q, doseq=True))
    return urlunparse(new)

PRICE_RE = re.compile(r"(?i)\s*(US\$|RD\$|\$)\s*([0-9\.\,]+)")

def parse_price(text: str):
    if not text: return (None, None)
    m = PRICE_RE.search(text.replace("\xa0", " ").strip())
    if not m: return (None, None)
    currency_raw, amount_raw = m.groups()
    currency = "USD" if "US" in currency_raw.upper() else "DOP"
    amount = float(re.sub(r"[^0-9\.]", "", amount_raw.replace(",", "")))
    return (currency, amount)

FUEL_OPTIONS = {"Gasolina","Diesel","Gasoil/diesel","Gas/GLP","Eléctrico","El\u00E9ctrico","Híbrido","H\u00EDbrido"}
COND_OPTIONS = {"Nuevo","Usado"}

def parse_fuel_and_condition(text: str):
    fuel = None; condition = None
    if not text: return fuel, condition
    parts = [p.strip() for p in text.split("-")]
    for p in parts:
        pl = p.lower()
        if p in FUEL_OPTIONS or any(k in pl for k in ["gasolina","diesel","elé","electr","híbr","glp"]):
            if pl.startswith("gasoil") or "diese" in pl: fuel = "Diesel"
            elif "glp" in pl: fuel = "GLP"
            elif "elé" in pl or "electr" in pl: fuel = "Eléctrico"
            elif "híbr" in pl: fuel = "Híbrido"
            elif "gasolina" in pl: fuel = "Gasolina"
        if p in COND_OPTIONS or "usado" in pl or "nuevo" in pl:
            condition = "Usado" if "usado" in pl else ("Nuevo" if "nuevo" in pl else condition)
    return fuel, condition

def get_base_root(base_url: str) -> str:
    p = urlparse(base_url)
    return f"{p.scheme}://{p.netloc}"

def normalize_url(href: str, base_root: str):
    if not href: return None
    if href.startswith("http"): return href
    return urljoin(base_root, href)

def fetch(url: str, ua: str):
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-DO,es;q=0.9,en;q=0.8",
        "Connection": "close",
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.text

def parse_listings(html: str, page_url: str, base_root: str):
    soup = BeautifulSoup(html, "lxml")
    cont = soup.select_one("#bigsearch-results-inner-results ul")
    if not cont: return []
    results = []
    for li in cont.select("li.normal"):
        classes = set(li.get("class", [])); classes.discard("normal")
        badges = sorted(list(classes)) if classes else []
        ad_id = li.get("data-id") or ""
        a = li.select_one("a[href]")
        href = normalize_url(a.get("href") if a else None, base_root)
        t1 = li.select_one(".title1")
        title = t1.get_text(strip=True) if t1 else None
        year = None
        y = li.select_one(".year")
        if y:
            ytxt = re.sub(r"[^0-9]", "", y.get_text())
            if ytxt:
                try: year = int(ytxt)
                except: year = None
        t2 = li.select_one(".title2")
        fuel, condition = parse_fuel_and_condition(t2.get_text(" ", strip=True) if t2 else "")
        p = li.select_one(".price")
        price_currency, price_amount = parse_price(p.get_text(" ", strip=True)) if p else (None, None)
        img = li.select_one("img.real")
        thumb = normalize_url(img.get("src"), base_root) if img else None
        photo_ids = []
        dphotos = li.get("data-photos")
        if dphotos: photo_ids = [x.strip() for x in dphotos.split(",") if x.strip()]
        results.append({
            "id": str(ad_id),
            "url": href,
            "title": title,
            "year": year,
            "fuel": fuel,
            "condition": condition,
            "price_currency": price_currency,
            "price_amount": price_amount,
            "thumbnail": thumb,
            "photo_ids": photo_ids,
            "badges": badges
        })
    return results

# --------- Detalle ---------

def to_mobile(url: str, base_url: str) -> str:
    if not url: return url
    p = urlparse(url)
    base = urlparse(base_url)
    netloc = base.netloc or p.netloc
    # construir subdominio móvil generando m.<dominio_sin_www>
    root = netloc[4:] if netloc.startswith("www.") else netloc
    m_netloc = f"m.{root}"
    return p._replace(scheme="https", netloc=m_netloc).geturl()

def extract_section_texts(soup: BeautifulSoup, title_regex: str):
    title = None
    for tag in soup.find_all(True):
        if tag.name in ("h1","h2","h3","h4","h5","strong","b"):
            if re.search(title_regex, tag.get_text(" ", strip=True), flags=re.I):
                title = tag; break
    if not title: return []
    lines = []
    for sib in title.find_all_next():
        if sib is title: continue
        if sib.name in ("h1","h2","h3","h4","h5","strong","b"): break
        if sib.name in ("ul","ol"):
            for li in sib.find_all("li"):
                t = li.get_text(" ", strip=True)
                if t: lines.append(t)
        elif sib.name in ("p","div","span","li"):
            t = sib.get_text(" ", strip=True)
            if t: lines.append(t)
    # únicos
    seen=set(); out=[]
    for t in lines:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

def parse_keyvals_from_block(text_lines):
    out = {}
    for raw in text_lines:
        t = raw.strip().strip("•").strip("-").strip()
        if not t or ":" not in t: 
            continue
        k, v = t.split(":", 1)
        k = re.sub(r"\s+", " ", k).strip()
        v = re.sub(r"\s+", " ", v).strip()
        if k and v: out[k] = v
    return out

PHONE_RE = re.compile(r"(?:\+?1?\s?(?:809|829|849))[\-\s\.]?\d{3}[\-\s\.]?\d{4}")

def parse_detail_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    datos_lines = extract_section_texts(soup, r"Datos\s+Generales")
    datos = parse_keyvals_from_block(datos_lines)
    acc_lines = extract_section_texts(soup, r"(Accesorios|Caracter\u00EDsticas|Características)")
    accesorios = sorted({re.sub(r"\s+", " ", t).strip("• ").strip() for t in acc_lines if t})
    obs_lines = extract_section_texts(soup, r"(Observaciones|Descripci\u00F3n|Descripción)")
    descripcion = "\n".join(obs_lines).strip() if obs_lines else None
    vend_lines = extract_section_texts(soup, r"(Vendedor|Contacto\s+Vendedor|Contacto\s+Dealer|Datos\s+del\s+Vendedor)")
    vendedor_text = " \n ".join(vend_lines) if vend_lines else None
    phones = sorted(set(PHONE_RE.findall(vendedor_text or "")))
    imgs = []
    for im in soup.select("img"):
        src = (im.get("src") or "").strip()
        if "AdsPhotos" in src:
            imgs.append(src)
    imgs = sorted(set(imgs))
    return {
        "general": datos,
        "accessories": accesorios,
        "description": descripcion,
        "vendor_text": vendedor_text,
        "phones": phones,
        "images": imgs
    }

def enrich_with_details(item: dict, ua: str, base_url: str, sleep_s: float) -> dict:
    url = item.get("url")
    if not url: return item
    murl = to_mobile(url, base_url)
    try:
        html = fetch(murl, ua)
        detail = parse_detail_page(html)
        item["detail"] = detail
    except Exception as e:
        item.setdefault("detail_error", str(e))
    time.sleep(sleep_s)
    return item

def main():
    cfg = load_config()
    base_url = cfg["base_url"]
    pages = int(cfg["pages"])
    sleep_s = float(cfg["sleep_seconds"])
    ua = cfg["user_agent"]
    base_root = get_base_root(base_url)

    all_items = []
    for i in range(pages):
        url = add_or_replace_query(base_url, PagingPageSkip=i)
        try:
            html = fetch(url, ua)
        except Exception as e:
            print(f"[WARN] Error al descargar página {i}: {e}", file=sys.stderr)
            break
        items = parse_listings(html, url, base_root)
        print(f"[INFO] Página {i}: {len(items)} items")
        if not items: break
        for it in items:
            it["scraped_at"] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            it["source"] = base_url
        all_items.extend(items)
        time.sleep(sleep_s)

    # Dedup por id
    seen=set(); dedup=[]
    for it in all_items:
        if it["id"] in seen: continue
        seen.add(it["id"]); dedup.append(it)
    all_items = dedup

    # Detalles
    if cfg.get("details", True):
        max_details = int(cfg.get("max_details", 120))
        d_sleep = float(cfg.get("detail_sleep_seconds", 0.8))
        count = 0
        for it in all_items:
            if count >= max_details: break
            enrich_with_details(it, ua, base_url, d_sleep)
            count += 1
        print(f"[INFO] Detalles descargados: {count}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "listings.json").write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")
    today = dt.datetime.utcnow().date().isoformat()
    (DATA_DIR / "daily").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "daily" / f"{today}.json").write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] Total anuncios: {len(all_items)} → data/listings.json")

if __name__ == "__main__":
    main()
