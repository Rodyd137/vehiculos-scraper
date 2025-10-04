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
    # no sobreescribir params ya presentes si el valor nuevo es None
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

def normalize_url(href: str | None, base_root: str):
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
        return []
    results = []
    # Tomar li con clase 'normal' o con alguna clase que empiece con 'promo-'
    for li in cont.select("li"):
        classes = set(li.get("class", []))
        if "normal" not in classes and not any(c.startswith("promo-") for c in classes):
            continue
        classes.discard("normal")
        badges = sorted([c for c in classes if c.startswith("promo-") or c.startswith("featured-")])

        ad_id = li.get("data-id") or ""
        if not ad_id:
            continue

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
        thumb = normalize_url(img.get("src") if img else None, base_root)

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

def to_desktop(url: str, base_url: str) -> str:
    if not url: return url
    p = urlparse(url)
    base = urlparse(base_url)
    netloc = p.netloc or base.netloc
    if netloc.startswith("m."):
        netloc = netloc[2:]
    return p._replace(scheme="https", netloc=netloc).geturl()

PHONE_RE = re.compile(r"(?:\+?1?\s?(?:809|829|849))[\-\s\.]?\d{3}[\-\s\.]?\d{4}")
SIZE_SEG = re.compile(r"/(\d{2,4})x(\d{2,4})/")

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

def upscale_adsphoto(u: str) -> str:
    """Si es AdsPhotos con tamaño embebido, fuerza 1200x800."""
    if "AdsPhotos" in u:
        u = SIZE_SEG.sub("/1200x800/", u)
    return u

def pick_best_img_url(tag) -> str | None:
    """Devuelve la mejor URL de un <img>: usa el mayor 'srcset', o data-* o src."""
    # 1) srcset → elegir el mayor width
    srcset = (tag.get("srcset") or "").strip()
    best = None
    best_w = -1
    if srcset:
        for part in srcset.split(","):
            part = part.strip()
            if not part:
                continue
            bits = part.split()
            url = bits[0]
            w = 0
            if len(bits) > 1 and bits[1].endswith("w"):
                try: w = int(bits[1][:-1])
                except: w = 0
            if w > best_w:
                best_w = w; best = url
    # 2) data-* o src
    if not best:
        for k in ("data-src","data-original","data-lazy","data-url","src"):
            v = (tag.get(k) or "").strip()
            if v:
                best = v
                break
    return best

def parse_detail_page(html: str, page_url: str, base_root: str):
    soup = BeautifulSoup(html, "lxml")

    # Bloques de texto (igual que antes)
    datos_lines = extract_section_texts(soup, r"Datos\s+Generales")
    datos = parse_keyvals_from_block(datos_lines)
    acc_lines = extract_section_texts(soup, r"(Accesorios|Caracter\u00EDsticas|Características)")
    accesorios = sorted({re.sub(r"\s+", " ", t).strip("• ").strip() for t in acc_lines if t})
    obs_lines = extract_section_texts(soup, r"(Observaciones|Descripci\u00F3n|Descripción)")
    descripcion = "\n".join(obs_lines).strip() if obs_lines else None
    vend_lines = extract_section_texts(soup, r"(Vendedor|Contacto\s+Vendedor|Contacto\s+Dealer|Datos\s+del\s+Vendedor)")
    vendedor_text = " \n ".join(vend_lines) if vend_lines else None
    phones = sorted(set(PHONE_RE.findall(vendedor_text or "")))

    # ====== Imágenes de alta ======
    imgs_set: set[str] = set()

    # a) src/srcset/data-* de <img>
    for im in soup.select("img"):
        u = pick_best_img_url(im)
        if not u:
            continue
        u = u.strip()
        if u.startswith("data:"):
            continue
        # normalizar relativo → absoluto
        u = normalize_url(u, base_root) or u
        # subir resolución si es AdsPhotos
        u = upscale_adsphoto(u)
        imgs_set.add(u)

    # b) Regex por si hay rutas en scripts (galería en JSON inline)
    for m in re.findall(r"https?://[^\s\"']*AdsPhotos/\d{2,4}x\d{2,4}/[^\s\"']+", html):
        imgs_set.add(upscale_adsphoto(m))

    imgs = sorted(imgs_set)

    return {
        "general": datos or None,
        "accessories": accesorios or None,
        "description": descripcion or None,
        "vendor_text": vendedor_text or None,
        "phones": phones or None,
        "images": imgs or None
    }

def enrich_with_details(item: dict, ua: str, base_url: str, sleep_s: float) -> dict:
    url = item.get("url")
    if not url: return item

    base_root = get_base_root(base_url)
    # Preferir ESCRITORIO; si falla o trae pocas fotos, probar MÓVIL
    candidates = [to_desktop(url, base_url), to_mobile(url, base_url)]
    merged = None
    for idx, u in enumerate(candidates):
        try:
            html = fetch(u, ua)
            detail = parse_detail_page(html, u, base_root)
            if not merged or len(detail.get("images") or []) > len(merged.get("images") or []):
                merged = detail
            # si ya logramos >=4 fotos, suficiente
            if len(merged.get("images") or []) >= 4:
                break
        except Exception as e:
            item.setdefault("detail_error", str(e))
        finally:
            time.sleep(sleep_s)

    if merged:
        item["detail"] = merged
        imgs_n = len(merged.get("images") or [])
        if imgs_n:
            print(f"[DETAIL] {item.get('id','?')}: {imgs_n} imágenes")
    return item

def main():
    cfg = load_config()
    base_url   = cfg["base_url"]
    if not base_url:
        print("[ERROR] Define 'base_url' en config.json (p. ej. https://www.tu-dominio.com/buscar)", file=sys.stderr)
        sys.exit(1)

    pages      = int(cfg["pages"])
    sleep_s    = float(cfg["sleep_seconds"])
    ua         = cfg["user_agent"]
    base_root  = get_base_root(base_url)

    order_col  = cfg.get("order_column", "Id")
    order_dir  = cfg.get("order_direction", "DESC")
    ipp        = int(cfg.get("items_per_page", 24))

    all_items = []
    seen_ids  = set()
    page = 0
    while True:
        if pages > 0 and page >= pages:
            break

        page_url = add_or_replace_query(
            base_url,
            PagingPageSkip=page,
            PagingItemsPerPage=ipp,
            OrderColumn=order_col,
            OrderDirection=order_dir
        )

        try:
            html = fetch(page_url, ua)
        except Exception as e:
            print(f"[WARN] Error al descargar página {page}: {e}", file=sys.stderr)
            break

        items = parse_listings(html, page_url, base_root)
        print(f"[INFO] Página {page}: {len(items)} items (antes de dedupe)")

        if not items:
            if page == 0:
                print("[INFO] 0 resultados en la primera página → fin")
            else:
                print(f"[INFO] Página {page} sin resultados → fin")
            break

        # merge por id
        nuevos = 0
        now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        for it in items:
            _id = it.get("id")
            if not _id or _id in seen_ids:
                continue
            seen_ids.add(_id)
            it["scraped_at"] = now_iso
            it["source"] = base_url
            all_items.append(it)
            nuevos += 1

        print(f"[INFO] Página {page}: nuevos {nuevos}, acumulado {len(all_items)}")

        # Heurística de parada: si la página no aportó nada nuevo
        if nuevos == 0:
            print("[INFO] Sin nuevos IDs en esta página → fin")
            break

        page += 1
        time.sleep(sleep_s if sleep_s >= 0 else 0)

    # Detalles (opcional)
    if cfg.get("details", True) and all_items:
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
