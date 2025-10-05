# scraper/main.py
from __future__ import annotations
import os, re, json, time, sys, pathlib, datetime as dt
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin
import requests
from bs4 import BeautifulSoup

# ==============================
# Paths / Config
# ==============================
ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

# Archivos/dirs incrementales (sharded)
ITEMS_DIR = DATA_DIR / "items"                         # un archivo por anuncio: items/<id>.json
ACTIVE_IDS_PATH = DATA_DIR / "active_ids.json"         # IDs activos detectados en la corrida
ACTIVE_LISTINGS_PATH = DATA_DIR / "listings_active.json"  # objetos activos (reconstruido desde items/)
PRUNE_REMOVED = True                                   # si True, borra archivos de anuncios que ya no están

CONFIG_PATH = ROOT / "config.json"
DEFAULT_CONFIG = {
    "base_url": "",                 # ej: "https://www.supercarros.com/buscar"
    "pages": 120,                   # máx. páginas; si <=0 hace auto hasta agotar
    "sleep_seconds": 2.0,           # pausa entre páginas
    "user_agent": "Mozilla/5.0 (compatible; VehiculosScraper/1.3)",
    "details": True,
    "detail_sleep_seconds": 0.8,
    "max_details": 120,
    "order_column": "Id",
    "order_direction": "DESC",
    "items_per_page": 24
}

# ✅ Fuentes adicionales (catálogos)
EXTRA_CATALOG_URLS = [
    "https://www.supercarros.com/v.pesados/",
    "https://www.supercarros.com/motores/",
]

# ==============================
# Config
# ==============================
def load_config():
    cfg = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg.update(json.load(f))
    # Overrides por env vars
    if "SC_MAX_PAGES" in os.environ:      cfg["pages"] = int(os.getenv("SC_MAX_PAGES", cfg["pages"]))
    if "SC_SLEEP_SECONDS" in os.environ:  cfg["sleep_seconds"] = float(os.getenv("SC_SLEEP_SECONDS", cfg["sleep_seconds"]))
    if "SC_DETAILS" in os.environ:        cfg["details"] = os.getenv("SC_DETAILS", "1") not in ("0","false","False")
    if "SC_DETAIL_SLEEP_SECONDS" in os.environ: cfg["detail_sleep_seconds"] = float(os.getenv("SC_DETAIL_SLEEP_SECONDS", cfg["detail_sleep_seconds"]))
    if "SC_MAX_DETAILS" in os.environ:    cfg["max_details"] = int(os.getenv("SC_MAX_DETAILS", cfg["max_details"]))
    return cfg

def add_or_replace_query(url: str, **params):
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            continue
        q[k] = str(v)
    new = p._replace(query=urlencode(q, doseq=True))
    return urlunparse(new)

# ==============================
# Parsers / Helpers
# ==============================
PRICE_RE = re.compile(r"(?i)\s*(US\$|RD\$|\$)\s*([0-9\.\,]+)")

def parse_price(text: str):
    if not text:
        return (None, None)
    m = PRICE_RE.search(text.replace("\xa0", " ").strip())
    if not m:
        return (None, None)
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
        if p in FUEL_OPTIONS or any(k in pl for k in ["gasolina","diesel","elé","electr","híbr","glp","gasoil"]):
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

def fetch(url: str, ua: str, retries: int = 2, backoff: float = 1.5) -> str:
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-DO,es;q=0.9,en;q=0.8",
        "Connection": "close",
    }
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(backoff * (attempt + 1))
    raise last_err

# ==============================
# Listing page
# ==============================
def parse_listings(html: str, page_url: str, base_root: str):
    soup = BeautifulSoup(html, "lxml")

    # Contenedor estándar
    cont = soup.select_one("#bigsearch-results-inner-results ul")

    # Fallback (v.pesados, motores): buscar li[data-id]
    if not cont:
        candidates = soup.select("li[data-id]")
        if not candidates:
            return []

        class FakeCont:
            def __init__(self, nodes): self._nodes = nodes
            def select(self, sel):
                if sel == "li": return self._nodes
                return []
        cont = FakeCont(candidates)

    results = []
    for li in cont.select("li"):
        ad_id = li.get("data-id") or ""
        if not ad_id:
            continue

        # badges (compat)
        classes = set(li.get("class", []))
        classes.discard("normal")
        badges = sorted([c for c in classes if c.startswith("promo-") or c.startswith("featured-")])

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

        img = li.select_one("img.real") or li.select_one("img")
        thumb = normalize_url(img.get("src") if img else None, base_root) if img else None

        dphotos = li.get("data-photos") or ""
        photo_ids = [x.strip() for x in dphotos.split(",") if x.strip()]

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

# ==============================
# Detail page (m.*)
# ==============================
def to_mobile(url: str, base_url: str) -> str:
    if not url: return url
    p = urlparse(url)
    base = urlparse(base_url)
    netloc = base.netloc or p.netloc
    root = netloc[4:] if netloc.startswith("www.") else netloc
    m_netloc = f"m.{root}"
    return p._replace(scheme="https", netloc=m_netloc).geturl()

PHONE_RE = re.compile(r"(?:\+?1?\s?(?:809|829|849))[\-\s\.]?\d{3}[\-\s\.]?\d{4}")
EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")

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

def pick_first_non_phone_line(lines: list[str]) -> str | None:
    for ln in lines:
        t = ln.strip()
        if not t:
            continue
        tl = t.lower()
        if PHONE_RE.search(t) or EMAIL_RE.search(t):
            continue
        if any(w in tl for w in ["tel", "tel.", "teléfono", "telefono", "whatsapp", "llamar", "llámanos"]):
            continue
        # Evitar direcciones obvias
        if any(w in tl for w in ["calle", "av.", "ave", "km", "sector", "provincia", "ciudad", "ubicación", "ubicacion", "dirección", "direccion"]):
            continue
        # Nombre “razonable”
        if len(t) <= 60 and re.search(r"[A-Za-zÁÉÍÓÚÑáéíóúñ]", t):
            return t
    return None

def extract_vendor_name(datos: dict, vend_lines: list[str]) -> str | None:
    # 1) Claves directas en "Datos Generales"
    for k in ["Vendedor","Nombre","Nombre del vendedor","Contacto","Contacto Vendedor","Propietario","Dealer"]:
        for key in list(datos.keys()):
            if k.lower() in key.lower():
                val = datos.get(key)
                if val and not PHONE_RE.search(val) and not EMAIL_RE.search(val):
                    return val.strip()
    # 2) En el bloque del vendedor
    #    Preferir "Nombre: X"
    for ln in vend_lines:
        if ":" in ln:
            k, v = ln.split(":", 1)
            if any(w in k.lower() for w in ["vendedor","nombre","contacto","propietario"]):
                v = v.strip()
                if v and not PHONE_RE.search(v) and not EMAIL_RE.search(v):
                    return v
    # 3) Primera línea no-telefono/ni-email/ni-dirección
    return pick_first_non_phone_line(vend_lines)

def extract_city(datos: dict, vend_lines: list[str], soup: BeautifulSoup) -> str | None:
    # 1) Claves típicas
    for k in ["Ciudad","Localidad","Ubicación","Ubicacion","Provincia","Sector"]:
        for key in list(datos.keys()):
            if k.lower() in key.lower():
                val = datos.get(key)
                if val and len(val.strip()) >= 2:
                    return val.strip()
    # 2) Líneas del bloque de vendedor con "Ciudad:" o "Ubicación:"
    for ln in vend_lines:
        if ":" in ln:
            k, v = ln.split(":", 1)
            if any(w in k.lower() for w in ["ciudad","ubicación","ubicacion","provincia","sector"]):
                v = v.strip()
                if v: return v
    # 3) Fallback: mirar spans/labels que contengan esas palabras
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(?i)(?:Ciudad|Ubicaci[oó]n|Provincia)\s*:\s*([A-Za-zÁÉÍÓÚÑáéíóúñ\.\-\s]+)", txt)
    if m:
        cand = m.group(1).strip()
        # Limpiar si hay trailing "Tel" etc
        cand = re.split(r"(?i)\b(Tel|Tel\.|Teléfono|Telefono)\b", cand)[0].strip(" -•:")
        if cand:
            return cand
    return None

def parse_detail_page(html: str):
    soup = BeautifulSoup(html, "lxml")

    datos_lines = extract_section_texts(soup, r"Datos\s+Generales")
    datos = parse_keyvals_from_block(datos_lines)

    acc_lines = extract_section_texts(soup, r"(Accesorios|Caracter\u00EDsticas|Características)")
    accesorios = sorted({re.sub(r"\s+", " ", t).strip("• ").strip() for t in acc_lines if t})

    obs_lines = extract_section_texts(soup, r"(Observaciones|Descripci\u00F3n|Descripción)")
    descripcion = "\n".join(obs_lines).strip() if obs_lines else None

    vend_lines = extract_section_texts(soup, r"(Vendedor|Contacto\s+Vendedor|Contacto\s+Dealer|Datos\s+del\s+Vendedor|Contacto)")
    vendedor_text = " \n ".join(vend_lines) if vend_lines else None

    phones = sorted(set(PHONE_RE.findall(vendedor_text or ""))) or None
    primary_phone = phones[0] if phones else None

    vendor_name = extract_vendor_name(datos, vend_lines) if vend_lines or datos else None
    city = extract_city(datos, vend_lines, soup)

    # Fotos
    imgs = []
    for im in soup.select("img"):
        src = (im.get("src") or "").strip()
        if "AdsPhotos" in src:
            imgs.append(src)
    imgs = sorted(set(imgs)) or None

    return {
        "general": datos or None,
        "accessories": accesorios or None,
        "description": descripcion or None,
        "vendor_text": vendedor_text or None,
        "vendor_name": vendor_name,
        "phones": phones,
        "primary_phone": primary_phone,
        "city": city,
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
    time.sleep(sleep_s if sleep_s >= 0 else 0)
    return item

# ==============================
# Incremental (sharded)
# ==============================
def safe_write_json(path: pathlib.Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def load_json_or(path: pathlib.Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def write_item_file(item: dict) -> None:
    """Guarda el anuncio en data/items/<id>.json si no existía o cambió."""
    _id = item.get("id")
    if not _id:
        return
    p = ITEMS_DIR / f"{_id}.json"
    if p.exists():
        try:
            old = json.loads(p.read_text(encoding="utf-8"))
            if old == item:
                return
        except Exception:
            pass
    safe_write_json(p, item)

def build_active_exports(active_ids: set[str]) -> None:
    """Genera índices de activos sin tocar los archivos por-id."""
    # 1) IDs
    safe_write_json(ACTIVE_IDS_PATH, sorted(active_ids))
    # 2) Objetos activos (leer cada <id>.json existente)
    listings = []
    for _id in sorted(active_ids):
        p = ITEMS_DIR / f"{_id}.json"
        if p.exists():
            try:
                listings.append(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                pass
    safe_write_json(ACTIVE_LISTINGS_PATH, listings)

def update_items_incremental(scraped_items: list[dict]) -> None:
    """
    Crea/actualiza solo archivos de anuncios nuevos y elimina los que ya no están
    (si PRUNE_REMOVED=True). Los que "vuelven activos" no se tocan.
    """
    ITEMS_DIR.mkdir(parents=True, exist_ok=True)

    # IDs activos detectados
    active_ids = {it.get("id") for it in scraped_items if it.get("id")}
    # IDs ya presentes en disco
    existing_ids = {p.stem for p in ITEMS_DIR.glob("*.json")}

    # Nuevos
    new_ids = active_ids - existing_ids
    # (también actualiza si cambió, por si quieres re-escritura mínima)
    for it in scraped_items:
        _id = it.get("id")
        if not _id:
            continue
        if _id in new_ids:
            write_item_file(it)
        else:
            # Si ya existe, opcionalmente compara y guarda si cambió
            write_item_file(it)

    # Removidos (no activos en esta corrida)
    if PRUNE_REMOVED:
        removed_ids = existing_ids - active_ids
        for _id in removed_ids:
            try:
                (ITEMS_DIR / f"{_id}.json").unlink(missing_ok=True)
            except Exception:
                pass

    # Índices de activos
    build_active_exports(active_ids)

# ==============================
# Scrape runner
# ==============================
def scrape_source(source_url: str, cfg: dict, seen_ids: set, all_items: list):
    pages      = int(cfg["pages"])
    sleep_s    = float(cfg["sleep_seconds"])
    ua         = cfg["user_agent"]
    base_root  = get_base_root(source_url)

    order_col  = cfg.get("order_column", "Id")
    order_dir  = cfg.get("order_direction", "DESC")
    ipp        = int(cfg.get("items_per_page", 24))

    page = 0
    while True:
        if pages > 0 and page >= pages:
            break

        page_url = add_or_replace_query(
            source_url,
            PagingPageSkip=page,
            PagingItemsPerPage=ipp,
            OrderColumn=order_col,
            OrderDirection=order_dir
        )

        try:
            html = fetch(page_url, ua)
        except Exception as e:
            print(f"[WARN] ({source_url}) Error al descargar página {page}: {e}", file=sys.stderr)
            break

        items = parse_listings(html, page_url, base_root)
        print(f"[INFO] ({source_url}) Página {page}: {len(items)} items (antes de dedupe)")

        if not items:
            if page == 0:
                print(f"[INFO] ({source_url}) 0 resultados en la primera página → fin")
            else:
                print(f"[INFO] ({source_url}) Página {page} sin resultados → fin")
            break

        nuevos = 0
        now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        for it in items:
            _id = it.get("id")
            if not _id or _id in seen_ids:
                continue
            seen_ids.add(_id)
            it["scraped_at"] = now_iso
            it["source"] = source_url
            all_items.append(it)
            nuevos += 1

        print(f"[INFO] ({source_url}) Página {page}: nuevos {nuevos}, acumulado {len(all_items)}")

        if nuevos == 0:
            print(f"[INFO] ({source_url}) Sin nuevos IDs en esta página → fin")
            break

        page += 1
        time.sleep(sleep_s if sleep_s >= 0 else 0)

def main():
    cfg = load_config()
    base_url   = cfg["base_url"]
    if not base_url:
        print("[ERROR] Define 'base_url' en config.json (p. ej. https://www.supercarros.com/buscar)", file=sys.stderr)
        sys.exit(1)

    # Fuentes: base + extras
    sources = [base_url] + EXTRA_CATALOG_URLS

    all_items: list[dict] = []
    seen_ids: set[str] = set()

    for src in sources:
        print(f"[INFO] >>> Iniciando scrape de fuente: {src}")
        scrape_source(src, cfg, seen_ids, all_items)

    # Detalles (opcional, limitado)
    if cfg.get("details", True) and all_items:
        max_details = int(cfg.get("max_details", 120))
        d_sleep = float(cfg.get("detail_sleep_seconds", 0.8))
        ua = cfg["user_agent"]
        count = 0
        for it in all_items:
            if count >= max_details: break
            enrich_with_details(it, ua, it.get("source") or base_url, d_sleep)
            count += 1
        print(f"[INFO] Detalles descargados: {count}")

    # === Salida incremental por archivos ===
    update_items_incremental(all_items)

    # (Opcional) snapshot monolítico de lo scrapeado en esta corrida:
    # DATA_DIR.mkdir(parents=True, exist_ok=True)
    # safe_write_json(DATA_DIR / "listings.json", all_items)

    # Snapshot diario
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = dt.datetime.utcnow().date().isoformat()
    (DATA_DIR / "daily").mkdir(parents=True, exist_ok=True)
    safe_write_json(DATA_DIR / "daily" / f"{today}.json", all_items)

    print(f"[DONE] Incremental: items/ actualizado, activos → {ACTIVE_LISTINGS_PATH.name}")

if __name__ == "__main__":
    main()
