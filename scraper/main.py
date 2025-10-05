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
    "user_agent": "Mozilla/5.0 (compatible; VehiculosScraper/1.3)",
    "details": True,
    "detail_sleep_seconds": 0.8,
    "max_details": 120,
    "order_column": "Id",
    "order_direction": "DESC",
    "items_per_page": 24
}

MASTER_PATH  = DATA_DIR / "listings_master.json"   # ← estado incremental
REMOVED_PATH = DATA_DIR / "listings_removed.json"  # ← tombstones

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
        if v is None: continue
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
            def select(self, sel): return self._nodes if sel == "li" else []
        cont = FakeCont(candidates)

    results = []
    for li in cont.select("li"):
        ad_id = li.get("data-id") or ""
        if not ad_id: continue

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

CITY_ALIASES = {
    # canónicas → variantes
    "Santo Domingo Este": {"santo domingo este"},
    "Santo Domingo Norte": {"santo domingo norte"},
    "Santo Domingo Oeste": {"santo domingo oeste"},
    "Santo Domingo": {"santo domingo", "d.n.", "distrito nacional", "dn"},
    "Santiago": {"santiago"},
    "San Cristóbal": {"san cristobal", "san cristóbal"},
    "San Pedro de Macorís": {"san pedro de macoris", "san pedro de macorís"},
    "La Vega": {"la vega"},
    "San Francisco de Macorís": {"san francisco de macoris", "san francisco de macorís"},
    "Bávaro": {"bavaro", "bávaro"},
    "Higüey": {"higuey", "higüey"},
    "La Romana": {"la romana"},
    "Puerto Plata": {"puerto plata"},
    "Moca": {"moca"},
}

def normalize_city(text: str) -> str | None:
    if not text: return None
    low = text.lower()
    for canon, variants in CITY_ALIASES.items():
        for v in variants:
            if v in low:
                return canon
    # fallback: palabra con mayúscula inicial + posible ‘de …’
    m = re.search(r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+de\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)", text)
    return m.group(1) if m else None

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

def guess_vendor_name(block_lines: list[str]) -> str | None:
    # 1) clave exacta
    for line in block_lines:
        m = re.search(r"(?i)^(?:vendedor|contacto|nombre)\s*:\s*(.+)$", line.strip())
        if m:
            cand = m.group(1).strip()
            if cand and not any(x in cand.lower() for x in ["tel", "cel", "correo", "email"]):
                return cand
    # 2) línea limpia (sin dígitos ni “tel/correo”)
    for line in block_lines:
        s = line.strip()
        if not s: continue
        if any(x in s.lower() for x in ["tel", "cel", "correo", "email", "ubicación", "ciudad", "sector", "dirección"]):
            continue
        if re.search(r"\d", s):  # tiene números → no es buen nombre
            continue
        if 2 <= len(s) <= 60:
            return s
    return None

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

    # nombre vendedor
    vendor_name = guess_vendor_name(vend_lines or []) or None

    # city por bloque vendedor o por datos generales
    city = None
    for line in (vend_lines or []):
        c = normalize_city(line)
        if c: city = c; break
    if not city:
        # buscar claves típicas en "Datos Generales"
        for k in ("Ciudad", "Ubicación", "Provincia", "Sector"):
            v = datos.get(k)
            c = normalize_city(v or "")
            if c:
                city = c; break

    primary_phone = phones[0] if phones else None

    # imágenes
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
        "vendor_name": vendor_name,
        "phones": phones or None,
        "primary_phone": primary_phone,
        "city": city,
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
    except Exception as e:
        item.setdefault("detail_error", str(e))
    time.sleep(sleep_s)
    return item

# ---------- Helpers estado incremental ----------
def load_state_dict(path: pathlib.Path) -> dict[str, dict]:
    if not path.exists(): return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Permitimos tanto lista como dict (back-compat)
        if isinstance(data, list):
            return {str(x.get("id")): x for x in data if isinstance(x, dict) and x.get("id")}
        elif isinstance(data, dict):
            return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    except Exception as e:
        print(f"[WARN] No se pudo leer {path.name}: {e}", file=sys.stderr)
    return {}

def save_json(path: pathlib.Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def compute_fingerprint(item: dict) -> str:
    # Campos “volátiles” de listado para detectar cambios relevantes
    keys = ["title","year","fuel","condition","price_currency","price_amount","thumbnail","url","badges","photo_ids"]
    snap = {k: item.get(k) for k in keys}
    return json.dumps(snap, sort_keys=True, ensure_ascii=False)

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

def update_master_incremental(current_items: list[dict], cfg: dict):
    """Actualiza listings_master.json y listings_removed.json de forma incremental."""
    now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Índices actuales
    current_by_id = {str(x["id"]): x for x in current_items if x.get("id")}

    # Estado previo
    master = load_state_dict(MASTER_PATH)
    removed = load_state_dict(REMOVED_PATH)

    changed_ids: list[str] = []  # nuevos o con fingerprint distinto

    # 1) Altas / Reapariciones / Actualizaciones
    for _id, cur in current_by_id.items():
        cur_fp = compute_fingerprint(cur)
        if _id in master:
            rec = master[_id]
            prev_fp = rec.get("fingerprint")
            if prev_fp != cur_fp:
                # Solo actualizamos campos de listado (no tocamos 'detail' ni primeras fechas)
                keys = ["title","year","fuel","condition","price_currency","price_amount","thumbnail","url","badges","photo_ids","source","scraped_at"]
                for k in keys:
                    rec[k] = cur.get(k)
                rec["fingerprint"] = cur_fp
                changed_ids.append(_id)
            rec["last_seen"] = now_iso
            rec["active"] = True
            master[_id] = rec
            if _id in removed:
                # Por si estaba en removed (no debería, pero por consistencia)
                removed.pop(_id, None)

        elif _id in removed:
            # Reaparece: NO TOCAMOS el payload salvo flags/fechas
            rec = removed.pop(_id)
            rec["active"] = True
            rec["reactivated_at"] = now_iso
            rec["last_seen"] = now_iso
            master[_id] = rec
            # No se agrega a changed_ids (no se vuelve a enriquecer)

        else:
            # Nuevo
            rec = cur.copy()
            rec["first_seen"] = now_iso
            rec["last_seen"] = now_iso
            rec["active"] = True
            rec["fingerprint"] = cur_fp
            master[_id] = rec
            changed_ids.append(_id)

    # 2) Bajas (mover al removed)
    missing_ids = [mid for mid in list(master.keys()) if mid not in current_by_id]
    for mid in missing_ids:
        rec = master.pop(mid)
        rec["active"] = False
        rec["inactive_since"] = now_iso
        removed[mid] = rec

    # 3) Enriquecer SOLO nuevos/cambiados (si procede)
    if cfg.get("details", True) and changed_ids:
        max_details = int(cfg.get("max_details", 120))
        d_sleep = float(cfg.get("detail_sleep_seconds", 0.8))
        ua = cfg["user_agent"]

        n = 0
        for mid in changed_ids:
            if n >= max_details: break
            try:
                base_url = master[mid].get("source") or cfg["base_url"]
                enriched = enrich_with_details(master[mid], ua, base_url, d_sleep)
                master[mid] = enriched
            except Exception as e:
                master[mid].setdefault("detail_error", str(e))
            n += 1
        print(f"[INFO] Detalles (incrementales) descargados: {n}")

    # 4) Guardar estado
    # Ordenar por last_seen desc para legibilidad
    master_list = sorted(master.values(), key=lambda x: x.get("last_seen",""), reverse=True)
    removed_list = sorted(removed.values(), key=lambda x: x.get("inactive_since",""), reverse=True)

    save_json(MASTER_PATH, master_list)
    save_json(REMOVED_PATH, removed_list)

    print(f"[STATE] master: {len(master_list)} activos+hist, removed: {len(removed_list)}")

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

    # === Estado incremental (master/removed) ===
    update_master_incremental(all_items, cfg)

    # === Detalles "legacy" (opcional): mantenemos tu salida clásica ===
    # Nota: esto en adelante no afecta al master incremental, solo a listados de la corrida
    if cfg.get("details", True) and all_items:
        max_details = int(cfg.get("max_details", 120))
        d_sleep = float(cfg.get("detail_sleep_seconds", 0.8))
        ua = cfg["user_agent"]
        count = 0
        for it in all_items:
            if count >= max_details: break
            enrich_with_details(it, ua, it.get("source") or base_url, d_sleep)
            count += 1
        print(f"[INFO] Detalles descargados (salida corrida): {count}")

    # === Salidas de la corrida (compatibilidad) ===
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "listings.json").write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")
    today = dt.datetime.utcnow().date().isoformat()
    (DATA_DIR / "daily").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "daily" / f"{today}.json").write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] Total anuncios descargados (corrida): {len(all_items)} → data/listings.json")

if __name__ == "__main__":
    main()
