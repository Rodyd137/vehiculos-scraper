# scraper/main.py
from __future__ import annotations
import os, re, json, time, sys, pathlib, datetime as dt
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin
import requests
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

CONFIG_PATH = ROOT / "config.json"
DEFAULT_CONFIG = {
    "base_url": "",                 # ej: "https://www.supercarros.com/buscar"
    "pages": 120,                   # máx. páginas; si <=0 hace auto hasta agotar
    "sleep_seconds": 2.0,           # pausa entre páginas
    "user_agent": "Mozilla/5.0 (compatible; VehiculosScraper/1.2)",
    "details": True,
    "detail_sleep_seconds": 0.8,
    "max_details": 120,
    "order_column": "Id",
    "order_direction": "DESC",
    "items_per_page": 24
}

# ✅ Nuevas fuentes adicionales (categorías)
EXTRA_CATALOG_URLS = [
    "https://www.supercarros.com/v.pesados/",
    "https://www.supercarros.com/motores/",
]

def load_config():
    cfg = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg.update(json.load(f))
    # Overrides por env vars (opcional)
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

def parse_listings(html: str, page_url: str, base_root: str):
    soup = BeautifulSoup(html, "lxml")
    cont = soup.select_one("#bigsearch-results-inner-results ul")
    if not cont:
        candidates = soup.select("li[data-id]")
        if not candidates:
            return []
        class FakeCont:
            def __init__(self, nodes): self._nodes = nodes
            def select(self, sel):
                if sel == "li":
                    return self._nodes
                return []
        cont = FakeCont(candidates)

    results = []
    for li in cont.select("li"):
        ad_id = li.get("data-id") or ""
        if not ad_id:
            continue

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

# --------- Detalle ---------
def to_mobile(url: str, base_url: str) -> str:
    if not url: return url
    p = urlparse(url)
    base = urlparse(base_url)
    netloc = base.netloc or p.netloc
    root = netloc[4:] if netloc.startswith("www.") else netloc
    m_netloc = f"m.{root}"
    return p._replace(scheme="https", netloc=m_netloc).geturl()

PHONE_RE = re.compile(r"(?:\+?1?\s?(?:809|829|849))[\-\s\.]?\d{3}[\-\s\.]?\d{4}")
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# Palabras que delatan líneas de contacto/labels (no nombre)
BAN_WORDS = {
    "tel", "tel.", "teléfono", "telefono", "whatsapp", "whatsap", "cel", "celular",
    "movil", "móvil", "email", "correo", "contacto", "vendedor", "dealer", "empresa",
    "concesionario", "horario", "dirección", "direccion", "ubicación", "ubicacion",
    "provincia", "ciudad", "sector", "localidad", "zona", "website", "web", "sitio",
    "santo domingo", "rd$", "us$", "precio", "id", "anuncio"
}

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

def _is_probable_name(t: str) -> bool:
    if not t: return False
    tt = t.strip()
    if not tt: return False
    if tt.endswith(":"): return False
    low = tt.lower()

    # Palabras vetadas (labels de contacto)
    if any(w in low for w in BAN_WORDS):
        return False

    # Evitar dígitos (si quieres permitir marcas con números, comenta esta línea)
    if re.search(r"\d", tt):
        return False

    # Longitud y número de palabras razonable
    words = [w for w in re.split(r"\s+", tt) if w]
    if len(words) == 0 or len(words) > 8:  # evita textos largos tipo direcciones
        return False
    if len(tt) < 3 or len(tt) > 64:
        return False

    # Tiene letras
    if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", tt):
        return False

    return True

def _pick_human_name(candidates: list[str]) -> str | None:
    for line in candidates:
        t = line.strip().strip("•").strip("-").strip()
        if _is_probable_name(t):
            return t
    return None

def _name_from_email(email: str) -> str | None:
    try:
        local = email.split("@",1)[0]
    except Exception:
        return None
    local = re.sub(r"[\._\-]+", " ", local)
    local = re.sub(r"\d+", "", local).strip()
    if not local:
        return None
    # Capitaliza palabras
    parts = [p for p in local.split() if p]
    if not parts: return None
    parts = [p.capitalize() for p in parts]
    name = " ".join(parts)
    # Validación rápida
    return name if _is_probable_name(name) else None

def _tail_lines(soup: BeautifulSoup, n: int = 50):
    text = soup.get_text("\n", strip=True)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    return lines[-n:]

def _guess_from_bottom(soup: BeautifulSoup):
    tail = _tail_lines(soup, n=60)
    first_phone = None
    phone_idx = None
    for idx, ln in enumerate(tail):
        m = PHONE_RE.search(ln)
        if m:
            first_phone = m.group(0)
            phone_idx = idx
            break
    vendor_name = None
    if first_phone is not None:
        prev_slice = list(reversed(tail[:phone_idx]))
        vendor_name = _pick_human_name(prev_slice)
    return vendor_name, first_phone, tail, phone_idx

def parse_detail_page(html: str):
    soup = BeautifulSoup(html, "lxml")

    # --- Datos generales / accesorios / descripción ---
    datos_lines = extract_section_texts(soup, r"Datos\s+Generales")
    datos = parse_keyvals_from_block(datos_lines)
    acc_lines = extract_section_texts(soup, r"(Accesorios|Caracter\u00EDsticas|Características)")
    accesorios = sorted({re.sub(r"\s+", " ", t).strip("• ").strip() for t in acc_lines if t})
    obs_lines = extract_section_texts(soup, r"(Observaciones|Descripci\u00F3n|Descripción)")
    descripcion = "\n".join(obs_lines).strip() if obs_lines else None

    # --- Vendedor: nombre / teléfonos / ciudad ---
    vend_lines = extract_section_texts(soup, r"(Vendedor|Contacto\s+Vendedor|Contacto\s+Dealer|Datos\s+del\s+Vendedor)")
    vend_kv = parse_keyvals_from_block(vend_lines)
    vendedor_text = " \n ".join(vend_lines) if vend_lines else None

    # Teléfonos de esa sección
    phones = sorted(set(PHONE_RE.findall(vendedor_text or "")))
    primary_phone = phones[0] if phones else None

    # Correos detectados para posible fallback de nombre
    emails_in_vendor = EMAIL_RE.findall(vendedor_text or "")

    # Nombre directo por claves
    vendor_name = (
        vend_kv.get("Nombre")
        or vend_kv.get("Vendedor")
        or vend_kv.get("Contacto")
        or vend_kv.get("Dealer")
        or vend_kv.get("Empresa")
        or vend_kv.get("Concesionario")
    )
    if not vendor_name and vend_lines:
        vendor_name = _pick_human_name(vend_lines)

    # Fallback móvil: buscar en el "tail"
    if not vendor_name or not primary_phone:
        name_guess, phone_guess, tail, idx = _guess_from_bottom(soup)
        if not vendor_name and name_guess:
            vendor_name = name_guess
        if not primary_phone and phone_guess:
            primary_phone = phone_guess
            phones = [primary_phone] + [p for p in phones if p != primary_phone]
        # Si no tenemos vendor_text, intenta armar contexto alrededor del teléfono
        if vendedor_text is None and idx is not None:
            lo = max(0, idx - 6); hi = min(len(tail), idx + 6)
            vendedor_text = "\n".join(tail[lo:hi])

    # Fallback por email → nombre (juan.perez → Juan Perez)
    if not vendor_name:
        # Usa emails de sección; si no, busca en todo el documento
        emails_all = emails_in_vendor or EMAIL_RE.findall(soup.get_text(" ", strip=True))
        if emails_all:
            vendor_name = _name_from_email(emails_all[0])

    # Ciudad (de 'Datos Generales' o de la sección de vendedor)
    city = None
    if datos:
        for k, v in datos.items():
            if re.search(r"(?i)(ciudad|ubicaci[oó]n|provincia|sector|localidad|zona)", k):
                city = v.split(",")[0].strip() if v else None
                if city: break
    if not city and vend_kv:
        for k, v in vend_kv.items():
            if re.search(r"(?i)(ciudad|ubicaci[oó]n|provincia|sector|localidad|zona)", k):
                city = v.split(",")[0].strip() if v else None
                if city: break
    if not city and vendedor_text:
        m = re.search(r"(?i)(?:Ciudad|Ubicaci[oó]n|Provincia)\s*:\s*([A-Za-zÁÉÍÓÚÜÑáéíóúüñ\-\s]+)", vendedor_text)
        if m:
            city = m.group(1).strip()

    # Imágenes
    imgs = []
    for im in soup.select("img"):
        src = (im.get("src") or "").strip()
        if "AdsPhotos" in src:
            imgs.append(src)
    imgs = sorted(set(imgs))

    return {
        "general": datos or None,
        "accessories": accesorios or None,
        "description": descripcion or None,
        "vendor_text": vendedor_text or None,
        "vendor_name": vendor_name or None,     # ← nombre más robusto
        "phones": phones or None,
        "primary_phone": primary_phone or None,
        "city": city or None,
        "images": imgs or None
    }

def enrich_with_details(item: dict, ua: str, base_url: str, sleep_s: float) -> dict:
    url = item.get("url")
    if not url: return item
    murl = to_mobile(url, base_url)
    try:
        html = fetch(murl, ua)
        detail = parse_detail_page(html)
        item["detail"] = detail

        # Campos de conveniencia a nivel item
        item["seller_name"]   = detail.get("vendor_name")
        item["primary_phone"] = detail.get("primary_phone") or (detail.get("phones") or [None])[0]
        item["city"]          = detail.get("city")
    except Exception as e:
        item.setdefault("detail_error", str(e))
    time.sleep(sleep_s)
    return item

# ---------- Scrape por cada fuente ----------
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

    # Detalles (opcional)
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

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "listings.json").write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")
    today = dt.datetime.utcnow().date().isoformat()
    (DATA_DIR / "daily").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "daily" / f"{today}.json").write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] Total anuncios: {len(all_items)} → data/listings.json")

if __name__ == "__main__":
    main()
