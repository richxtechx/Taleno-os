"""
TΛLENO OS — panel de cerebros de RichTech
Cerebro 1 (Radar): escucha comentarios y ordena dolores, objeciones y deseos.
Cerebro 2 (Analista): del dolor al producto, informe de 8 pasos con veredicto.
Fuentes: texto pegado, YouTube (búsqueda o video) y Facebook (Apify)

Fuentes:
  - Enlace de Facebook  -> Apify (facebook-comments-scraper)
  - Enlace de YouTube   -> YouTube Data API v3
  - Palabra clave       -> YouTube Data API v3 (busca videos y junta sus comentarios)
Luego todo pasa por Gemini y devuelve JSON con dolores, objeciones y deseos.
"""

import asyncio
import logging
import os
import re
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import List, Literal, Optional

import httpx
from apify_client import ApifyClientAsync
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from itsdangerous import BadSignature, URLSafeTimedSerializer
from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from pydantic import BaseModel, Field, HttpUrl

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("comment-analyzer")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
GEMINI_FALLBACK_MODEL = os.getenv("GEMINI_FALLBACK_MODEL", "gemini-flash-lite-latest")
MAX_COMMENTS = int(os.getenv("MAX_COMMENTS", "150"))          # Facebook
YT_COMMENTS_PER_VIDEO = int(os.getenv("YT_COMMENTS_PER_VIDEO", "100"))
MAX_COMMENT_CHARS = 600
MAX_PASTED = int(os.getenv("MAX_PASTED", "400"))                 # tope de comentarios pegados
APIFY_ACTOR_ID = "apify/facebook-comments-scraper"
YT_API = "https://www.googleapis.com/youtube/v3"

APP_NAME = os.getenv("APP_NAME", "TΛLENO OS")
APP_VERSION = "v25-recuperar-clave"      # se ve en /health, para saber qué versión está desplegada
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "richard@richardtaleno.com")
TELEGRAM_URL = os.getenv("TELEGRAM_URL", "")             # ej: https://t.me/tucanal
ANIO = datetime.now(timezone.utc).year
SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_urlsafe(32)
SESSION_DAYS = 30
COOKIE_NAME = "taleno_session"
GUEST_PREFIX = "invitado:"
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "1") != "0"   # ponlo en 0 solo para probar en local (http)
GUEST_MODE = os.getenv("GUEST_MODE", "0") == "1"         # 1 = permite entrar sin cuenta
GUEST_FULL = os.getenv("GUEST_FULL", "0") == "1"         # 1 = los invitados también pueden usar Facebook (cuesta Apify)
GUEST_LIMIT = int(os.getenv("GUEST_LIMIT", "3"))         # créditos de regalo al registrarse
VENTA_URL = os.getenv("VENTA_URL", "")                   # página de venta de créditos (systeme.io)
RESEND_API_KEY = os.getenv("RESEND_API_KEY")             # envío del correo de recuperación
MAIL_FROM = os.getenv("MAIL_FROM", "TΛLENO OS <acceso@richardtaleno.com>")
APP_URL = os.getenv("APP_URL", "https://taleno-app.onrender.com")
MINUTOS_ENLACE = 30
RESEND_API_KEY = os.getenv("RESEND_API_KEY")             # envío del enlace de acceso
MAIL_FROM = os.getenv("MAIL_FROM", "TΛLENO OS <acceso@richardtaleno.com>")
APP_URL = os.getenv("APP_URL", "https://taleno-app.onrender.com")
MINUTOS_ENLACE = 15
COSTO_CREDITOS = {"rapido": 0, "mercado": 1, "copy": 1}  # el Radar no gasta créditos
COPY_GUEST_LIMIT = int(os.getenv("COPY_GUEST_LIMIT", "1"))  # piezas de copy gratis por correo
GUEST_MODEL = os.getenv("GUEST_MODEL", "gemini-3.5-flash-lite")  # modelo barato para invitados
DATA_DIR = os.getenv("DATA_DIR", "/tmp")                 # carpeta donde se guardan los leads
LEADS_FILE = os.path.join(DATA_DIR, "leads.json")
SYSTEME_API_KEY = os.getenv("SYSTEME_API_KEY")           # clave de systeme.io (Settings → Public API keys)
SYSTEME_TAG_ID = os.getenv("SYSTEME_TAG_ID")             # id del tag que quieres aplicar (opcional)
SYSTEME_API = "https://api.systeme.io/api"
SUPABASE_URL = os.getenv("SUPABASE_URL")                 # https://xxxx.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY")                 # clave service_role (nunca en el repo)

def _load_users() -> dict:
    """APP_USERS="rich@correo.com:clave,ana@correo.com:otra". Alternativa simple: APP_PASSWORD."""
    users = {}
    raw = os.getenv("APP_USERS", "")
    for pair in raw.split(","):
        if ":" in pair:
            email, _, pwd = pair.partition(":")
            if email.strip() and pwd.strip():
                users[email.strip().lower()] = pwd.strip()
    single = os.getenv("APP_PASSWORD", "")
    if single and not users:
        users[os.getenv("APP_USER", "admin").strip().lower()] = single.strip()
    return users

USERS = _load_users()
signer = URLSafeTimedSerializer(SECRET_KEY, salt="sesion")

app = FastAPI(
    title=APP_NAME,
    description="Panel de cerebros de RichTech: escucha comentarios reales y los convierte en decisiones.",
    version="3.0.0",
)


# ---------------------------------------------------------------------------
# Sesión y login
# ---------------------------------------------------------------------------
def current_user(request: Request) -> Optional[str]:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    try:
        return signer.loads(token, max_age=SESSION_DAYS * 86400)
    except BadSignature:
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Leads: correos capturados, usos y feedback
# ---------------------------------------------------------------------------
_leads_lock = asyncio.Lock()
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$")
USA_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)


# --- Supabase (si está configurado) ---------------------------------------
def _sb_headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }


_sb_ultimo_error = {"detalle": ""}


def _sb_base() -> str:
    """Base del proyecto, aunque la variable traiga /rest/v1 o barras de más."""
    base = (SUPABASE_URL or "").strip().strip('"').strip("'").rstrip("/")
    for sufijo in ("/rest/v1/leads", "/rest/v1", "/rest"):
        if base.endswith(sufijo):
            base = base[: -len(sufijo)]
            break
    return base.rstrip("/")


def _sb_tabla(tabla: str, metodo: str, params: dict = None, payload=None) -> list:
    url = f"{_sb_base()}/rest/v1/{tabla}"
    try:
        with httpx.Client(timeout=15, headers=_sb_headers()) as client:
            resp = client.request(metodo, url, params=params, json=payload)
        if resp.status_code >= 400:
            _sb_ultimo_error["detalle"] = f"{tabla} {metodo} {resp.status_code}: {resp.text[:300]}"
            logger.warning("Supabase %s %s -> %s: %s", tabla, metodo, resp.status_code, resp.text[:300])
            return []
        _sb_ultimo_error["detalle"] = ""
        return resp.json() if resp.text else []
    except Exception as exc:
        _sb_ultimo_error["detalle"] = f"Error de conexión: {exc}"
        logger.exception("Supabase: error de conexión")
        return []


def _sb_url() -> str:
    """Arma la URL de la tabla de leads."""
    return f"{_sb_base()}/rest/v1/leads"


def _sb(metodo: str, params: dict = None, payload=None) -> list:
    return _sb_tabla("leads", metodo, params, payload)


# --- Archivo local (respaldo cuando no hay Supabase) ----------------------
def _read_file_leads() -> dict:
    try:
        with open(LEADS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_file_leads(data: dict):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(LEADS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
    except Exception:
        logger.exception("No se pudieron guardar los leads")


# --- API interna: igual para los dos almacenes ----------------------------
def _lead_nuevo(email: str, nombre: str = "") -> dict:
    return {"email": email, "nombre": nombre, "creado": datetime.now(timezone.utc).isoformat(),
            "usos": 0, "systeme": False, "feedback": [],
            "creditos": GUEST_LIMIT, "ilimitado": False}


def _read_leads() -> dict:
    """Todos los leads, indexados por correo."""
    if USA_SUPABASE:
        filas = _sb("GET", params={"select": "*", "order": "creado.desc"})
        return {f["email"]: f for f in filas if f.get("email")}
    return _read_file_leads()


def lead_get(email: str) -> Optional[dict]:
    if USA_SUPABASE:
        filas = _sb("GET", params={"email": f"eq.{email}", "select": "*", "limit": 1})
        return filas[0] if filas else None
    return _read_file_leads().get(email)


def _lead_upsert(lead: dict):
    if USA_SUPABASE:
        _sb("POST", payload=lead)
        return
    data = _read_file_leads()
    data[lead["email"]] = lead
    _write_file_leads(data)


def creditos_de(email: str) -> int:
    lead = lead_get(email)
    if not lead:
        return GUEST_LIMIT
    if lead.get("ilimitado"):
        return -1                      # -1 = sin límite
    return int(lead.get("creditos", GUEST_LIMIT) or 0)


async def consumir_credito(email: str, cuantos: int = 1) -> int:
    """Descuenta créditos y devuelve los que quedan (-1 si es ilimitado)."""
    async with _leads_lock:
        lead = lead_get(email) or _lead_nuevo(email)
        if lead.get("ilimitado"):
            return -1
        lead["creditos"] = max(int(lead.get("creditos", GUEST_LIMIT) or 0) - cuantos, 0)
        lead["usos"] = int(lead.get("usos", 0) or 0) + 1
        lead["ultimo_uso"] = datetime.now(timezone.utc).isoformat()
        _lead_upsert(lead)
        return lead["creditos"]


async def cargar_creditos(email: str, cuantos: int) -> int:
    async with _leads_lock:
        lead = lead_get(email) or _lead_nuevo(email)
        lead["creditos"] = max(int(lead.get("creditos", 0) or 0) + cuantos, 0)
        _lead_upsert(lead)
        return lead["creditos"]


async def marcar_ilimitado(email: str, valor: bool):
    async with _leads_lock:
        lead = lead_get(email) or _lead_nuevo(email)
        lead["ilimitado"] = valor
        _lead_upsert(lead)


def lead_borrar(email: str, con_analisis: bool = True) -> dict:
    """Borra el usuario y, si se pide, todos sus análisis."""
    resultado = {"usuario": False, "analisis": 0}
    if USA_SUPABASE:
        if con_analisis:
            filas = _sb_tabla("analisis", "GET", params={
                "usuario": f"eq.{GUEST_PREFIX}{email}", "select": "id", "limit": 500})
            resultado["analisis"] = len(filas)
            _sb_tabla("analisis", "DELETE", params={"usuario": f"eq.{GUEST_PREFIX}{email}"})
        _sb_tabla("leads", "DELETE", params={"email": f"eq.{email}"})
        resultado["usuario"] = not _sb_ultimo_error["detalle"]
    else:
        data = _read_file_leads()
        resultado["usuario"] = data.pop(email, None) is not None
        _write_file_leads(data)
    return resultado


def lead_usos(email: str) -> int:
    lead = lead_get(email)
    return int((lead or {}).get("usos", 0) or 0)


async def lead_guardar(email: str, nombre: str) -> dict:
    async with _leads_lock:
        lead = lead_get(email) or _lead_nuevo(email, nombre)
        if nombre:
            lead["nombre"] = nombre
        _lead_upsert(lead)
        return lead


async def lead_consumir_uso(email: str) -> int:
    """Suma un uso y devuelve cuántos le quedan."""
    async with _leads_lock:
        lead = lead_get(email) or _lead_nuevo(email)
        lead["usos"] = int(lead.get("usos", 0) or 0) + 1
        lead["ultimo_uso"] = datetime.now(timezone.utc).isoformat()
        _lead_upsert(lead)
        return max(GUEST_LIMIT - lead["usos"], 0)


async def lead_marcar_systeme(email: str, ok: bool):
    async with _leads_lock:
        lead = lead_get(email)
        if not lead:
            return
        lead["systeme"] = ok
        _lead_upsert(lead)


# --- Historial de análisis (solo con Supabase) ----------------------------
def historial_guardar(usuario: str, agente: str, titulo: str, fuente: str,
                      total: int, datos: dict) -> Optional[str]:
    if not USA_SUPABASE:
        return None
    fila = {"usuario": usuario, "agente": agente, "titulo": titulo[:200],
            "fuente": fuente[:100], "total_comentarios": total, "datos": datos}
    creado = _sb_tabla("analisis", "POST", payload=fila)
    return (creado[0].get("id") if creado else None)


def historial_listar(usuario: str, limite: int = 50) -> list:
    if not USA_SUPABASE:
        return []
    base = {"usuario": f"eq.{usuario}",
            "select": "id,agente,titulo,fuente,total_comentarios,creado",
            "limit": limite}
    filas = _sb_tabla("analisis", "GET", params={**base, "order": "creado.desc"})
    if not filas and _sb_ultimo_error["detalle"]:
        logger.warning("Historial: reintento sin ordenar (%s)", _sb_ultimo_error["detalle"])
        filas = _sb_tabla("analisis", "GET", params=base)
        filas = sorted(filas, key=lambda f: f.get("creado") or "", reverse=True)
    return filas


def historial_abrir(usuario: str, id_analisis: str, admin: bool = False) -> Optional[dict]:
    if not USA_SUPABASE:
        return None
    params = {"id": f"eq.{id_analisis}", "select": "*", "limit": 1}
    if not admin:
        params["usuario"] = f"eq.{usuario}"
    filas = _sb_tabla("analisis", "GET", params=params)
    return filas[0] if filas else None


ULTIMA_NOVEDAD = {"fecha": "", "revisado": 0.0}


def ultima_novedad() -> str:
    """Fecha de la novedad más reciente, cacheada 5 minutos."""
    import time
    ahora = time.time()
    if ahora - ULTIMA_NOVEDAD["revisado"] > 300:
        ULTIMA_NOVEDAD["revisado"] = ahora
        filas = novedades_listar(1)
        ULTIMA_NOVEDAD["fecha"] = (filas[0].get("creado") or "") if filas else ""
    return ULTIMA_NOVEDAD["fecha"]


def novedades_listar(limite: int = 30) -> list:
    if not USA_SUPABASE:
        return []
    filas = _sb_tabla("novedades", "GET", params={
        "select": "id,titulo,texto,creado", "order": "creado.desc", "limit": limite})
    if not filas and _sb_ultimo_error["detalle"]:
        filas = _sb_tabla("novedades", "GET", params={"select": "id,titulo,texto,creado", "limit": limite})
        filas = sorted(filas, key=lambda f: f.get("creado") or "", reverse=True)
    return filas


def novedades_crear(titulo: str, texto: str) -> Optional[str]:
    if not USA_SUPABASE:
        return None
    creada = _sb_tabla("novedades", "POST", payload={"titulo": titulo[:150], "texto": texto[:4000]})
    return creada[0].get("id") if creada else None


def novedades_borrar(id_novedad: str):
    if USA_SUPABASE:
        _sb_tabla("novedades", "DELETE", params={"id": f"eq.{id_novedad}"})


def material_listar(limite: int = 20) -> list:
    """Los últimos análisis con su contenido, para extraer material de posts."""
    if not USA_SUPABASE:
        return []
    filas = _sb_tabla("analisis", "GET", params={
        "select": "id,usuario,agente,titulo,creado,datos",
        "order": "creado.desc", "limit": limite})
    if not filas and _sb_ultimo_error["detalle"]:
        filas = _sb_tabla("analisis", "GET", params={
            "select": "id,usuario,agente,titulo,creado,datos", "limit": limite})
        filas = sorted(filas, key=lambda f: f.get("creado") or "", reverse=True)
    return filas


PALABRAS_VACIAS = {"de","del","la","el","los","las","un","una","unos","unas","en","para","por","con","y","o",
                   "que","qué","como","cómo","mi","mis","tu","tus","su","sus","se","lo","al","a","the","of",
                   "es","son","sin","sobre","más","mas","muy","ya","me","te","le"}


def _palabras_nicho(texto: str) -> set:
    """Palabras significativas del tema: minúsculas, sin tildes, sin palabras vacías."""
    import unicodedata
    base = unicodedata.normalize("NFKD", (texto or "").lower())
    base = "".join(c for c in base if not unicodedata.combining(c))
    return {p for p in re.split(r"[^a-z0-9ñ]+", base) if p and p not in PALABRAS_VACIAS and len(p) > 2}


def _mismo_nicho(a: set, b: set) -> bool:
    """Dos temas son el mismo si comparten lo esencial (no hace falta que coincidan palabra por palabra)."""
    if not a or not b:
        return a == b
    comunes = len(a & b)
    return comunes >= 2 or comunes == min(len(a), len(b))


def _nicho_de(fila: dict) -> str:
    """El tema que la persona buscó, tal como lo escribió."""
    datos = fila.get("datos") or {}
    consulta = (datos.get("consulta") or "").strip()
    if consulta and consulta != "texto pegado" and not consulta.startswith("http"):
        return consulta[:90]
    return (fila.get("titulo") or "Sin tema")[:90]


def agrupar_nichos(filas: list) -> list:
    """Junta los análisis por tema, tolerando que cada quien lo escriba distinto."""
    grupos = []
    for f in filas:
        if f.get("agente") == "copy":
            continue
        etiqueta = _nicho_de(f)
        palabras = _palabras_nicho(etiqueta)
        destino = next((g for g in grupos if _mismo_nicho(g["palabras"], palabras)), None)
        if destino is None:
            destino = {"etiqueta": etiqueta, "palabras": set(), "analisis": [], "personas": set(),
                       "agentes": set(), "primera": "", "ultima": ""}
            grupos.append(destino)
        destino["palabras"] |= palabras
        destino["analisis"].append(f)
        destino["personas"].add(f.get("usuario", ""))
        destino["agentes"].add(f.get("agente", ""))
        creado = f.get("creado") or ""
        destino["ultima"] = max(destino["ultima"], creado)
        destino["primera"] = min(destino["primera"] or creado, creado)
        if len(etiqueta) < len(destino["etiqueta"]):
            destino["etiqueta"] = etiqueta
    return sorted(grupos, key=lambda g: (len(g["personas"]), len(g["analisis"]), g["ultima"]), reverse=True)


def extraer_material(datos: dict) -> dict:
    """Saca de un informe solo lo que sirve para escribir: frases, temas y el problema."""
    out = {"frases": [], "dolores": [], "objeciones": [], "deseos": [], "problema": "", "producto": ""}
    m = datos.get("mercado")
    a = datos.get("analisis")
    if m:
        for f in (m.get("frases_repetidas") or []):
            if f.get("frase"):
                out["frases"].append(f["frase"])
        for i in (m.get("intentos_fallidos") or []):
            if i.get("ejemplo"):
                out["frases"].append(i["ejemplo"])
        for cita in ((m.get("dolor_emocional") or {}).get("evidencia") or []):
            out["frases"].append(cita)
        for cita in ((m.get("resultado_deseado") or {}).get("evidencia") or []):
            out["frases"].append(cita)
        pu = m.get("problema_urgente") or {}
        out["problema"] = pu.get("problema_urgente_especifico", "")
        out["producto"] = (m.get("propuesta_producto") or {}).get("nombre", "")
        out["dolores"] = [i.get("que_intentaron", "") for i in (m.get("intentos_fallidos") or [])]
    if a:
        for clave in ("dolores", "objeciones", "deseos"):
            for item in (a.get(clave) or []):
                if item.get("tema"):
                    out[clave].append(item["tema"])
                for cita in (item.get("ejemplos") or []):
                    out["frases"].append(cita)
    # sin repetidas, conservando el orden
    vistas = set()
    out["frases"] = [f for f in out["frases"] if not (f in vistas or vistas.add(f))][:12]
    return out


def actividad_listar(limite: int = 200) -> list:
    """Todos los análisis de todos los usuarios (solo para el dueño)."""
    if not USA_SUPABASE:
        return []
    filas = _sb_tabla("analisis", "GET", params={
        "select": "id,usuario,agente,titulo,fuente,total_comentarios,creado",
        "order": "creado.desc", "limit": limite,
    })
    if not filas and _sb_ultimo_error["detalle"]:
        filas = _sb_tabla("analisis", "GET", params={
            "select": "id,usuario,agente,titulo,fuente,total_comentarios,creado", "limit": limite})
        filas = sorted(filas, key=lambda f: f.get("creado") or "", reverse=True)
    return filas


def historial_borrar(usuario: str, id_analisis: str):
    if USA_SUPABASE:
        _sb_tabla("analisis", "DELETE", params={"id": f"eq.{id_analisis}", "usuario": f"eq.{usuario}"})


async def lead_feedback(email: str, util: Optional[bool], texto: str):
    async with _leads_lock:
        lead = lead_get(email)
        if not lead:
            return
        historial = lead.get("feedback") or []
        historial.append({"util": util, "texto": texto[:1000],
                          "fecha": datetime.now(timezone.utc).isoformat()})
        lead["feedback"] = historial
        _lead_upsert(lead)


async def _systeme_tag_id(client: httpx.AsyncClient) -> Optional[int]:
    """Acepta el id numérico o el NOMBRE del tag; si es nombre, lo busca en systeme.io."""
    if not SYSTEME_TAG_ID:
        return None
    valor = SYSTEME_TAG_ID.strip()
    if valor.isdigit():
        return int(valor)
    resp = await client.get(f"{SYSTEME_API}/tags", params={"limit": 100})
    if resp.status_code == 200:
        for tag in resp.json().get("items", []):
            if str(tag.get("name", "")).strip().lower() == valor.lower():
                return int(tag["id"])
    logger.warning("systeme.io: no encontré el tag '%s' (%s)", valor, resp.status_code)
    return None


async def _systeme_asignar_tag(client: httpx.AsyncClient, contacto_id, tag_id: int) -> tuple:
    """Prueba el formato numérico y, si lo rechaza, el de texto."""
    for cuerpo in ({"tagId": tag_id}, {"tagId": str(tag_id)}):
        resp = await client.post(f"{SYSTEME_API}/contacts/{contacto_id}/tags", json=cuerpo)
        if resp.status_code in (200, 201, 204):
            return True, resp.status_code, ""
        detalle = resp.text[:300]
    return False, resp.status_code, detalle


async def systeme_sync(email: str, nombre: str) -> dict:
    """Crea el contacto en systeme.io y le aplica el tag. Devuelve un diagnóstico."""
    info = {"contacto": False, "tag": False, "detalle": ""}
    if not SYSTEME_API_KEY:
        info["detalle"] = "Falta SYSTEME_API_KEY"
        return info

    headers = {"X-API-Key": SYSTEME_API_KEY, "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=20, headers=headers) as client:
            resp = await client.post(
                f"{SYSTEME_API}/contacts",
                json={"email": email, "fields": [{"slug": "first_name", "value": nombre}]},
            )
            contacto_id = resp.json().get("id") if resp.status_code in (200, 201) else None

            if not contacto_id:
                busca = await client.get(f"{SYSTEME_API}/contacts", params={"email": email})
                items = busca.json().get("items", []) if busca.status_code == 200 else []
                if items:
                    contacto_id = items[0].get("id")
                else:
                    info["detalle"] = f"No se pudo crear ni encontrar el contacto ({resp.status_code}): {resp.text[:200]}"
                    logger.warning("systeme.io: %s", info["detalle"])
                    return info

            info["contacto"] = True
            info["contacto_id"] = contacto_id

            tag_id = await _systeme_tag_id(client)
            if tag_id is None:
                info["detalle"] = "Sin tag configurado o nombre no encontrado"
                return info

            ok, codigo, detalle = await _systeme_asignar_tag(client, contacto_id, tag_id)
            info["tag"] = ok
            info["tag_id"] = tag_id
            if not ok:
                info["detalle"] = f"El tag falló ({codigo}): {detalle}"
                logger.warning("systeme.io: %s", info["detalle"])
            return info
    except Exception as exc:
        info["detalle"] = f"Error de conexión: {exc}"
        logger.exception("systeme.io: error enviando el contacto")
        return info


recuperacion_signer = URLSafeTimedSerializer(SECRET_KEY, salt="recuperar")
USA_CORREO = bool(RESEND_API_KEY)
ITERACIONES = 120_000


def hash_clave(clave: str, sal: Optional[str] = None) -> str:
    """Guardamos la contraseña cifrada, nunca en texto plano."""
    import hashlib
    sal = sal or secrets.token_hex(16)
    derivada = hashlib.pbkdf2_hmac("sha256", clave.encode("utf-8"), sal.encode("utf-8"), ITERACIONES)
    return f"pbkdf2${ITERACIONES}${sal}${derivada.hex()}"


def clave_correcta(clave: str, guardado: str) -> bool:
    try:
        _, iteraciones, sal, _ = guardado.split("$")
        import hashlib
        derivada = hashlib.pbkdf2_hmac("sha256", clave.encode("utf-8"), sal.encode("utf-8"), int(iteraciones))
        return secrets.compare_digest(f"pbkdf2${iteraciones}${sal}${derivada.hex()}", guardado)
    except Exception:
        return False


async def guardar_clave(email: str, clave: str):
    async with _leads_lock:
        lead = lead_get(email) or _lead_nuevo(email)
        lead["pass_hash"] = hash_clave(clave)
        _lead_upsert(lead)


async def guardar_nonce(email: str, nonce: Optional[str]):
    async with _leads_lock:
        lead = lead_get(email)
        if not lead:
            return
        lead["acceso_nonce"] = nonce
        _lead_upsert(lead)


def _correo_recuperacion(enlace: str, nombre: str) -> str:
    saludo = f"Hola {nombre}," if nombre else "Hola,"
    return f"""<div style="font-family: Arial, Helvetica, sans-serif; font-size: 16px; color: #0b0b0f; line-height: 1.5">
      <p>{saludo}</p>
      <p>Pediste cambiar tu contraseña de TΛLENO OS. Entra aquí y elige una nueva:</p>
      <p style="margin: 24px 0"><a href="{enlace}">{enlace}</a></p>
      <p>El enlace vale {MINUTOS_ENLACE} minutos y se usa una sola vez.</p>
      <p style="font-size: 13px; color: #6b7280">Si no pediste el cambio, ignora este correo: tu contraseña sigue igual.</p>
      <p style="margin-top: 24px">Richard Taleno</p>
    </div>"""


async def enviar_recuperacion(email: str) -> bool:
    lead = lead_get(email)
    if not lead or not RESEND_API_KEY:
        return False
    nonce = secrets.token_urlsafe(16)
    await guardar_nonce(email, nonce)
    token = recuperacion_signer.dumps({"email": email, "nonce": nonce})
    enlace = f"{APP_URL.rstrip('/')}/recuperar?t={token}"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json={"from": MAIL_FROM, "to": [email],
                      "subject": "Cambiar tu contraseña de TΛLENO OS",
                      "html": _correo_recuperacion(enlace, lead.get("nombre", ""))},
            )
        if resp.status_code >= 400:
            logger.warning("Resend %s: %s", resp.status_code, resp.text[:300])
            return False
        return True
    except Exception:
        logger.exception("Resend: error enviando la recuperación")
        return False


async def validar_recuperacion(token: str) -> Optional[str]:
    try:
        datos = recuperacion_signer.loads(token, max_age=MINUTOS_ENLACE * 60)
    except Exception:
        return None
    email = (datos or {}).get("email", "").strip().lower()
    nonce = (datos or {}).get("nonce")
    lead = lead_get(email) if email else None
    if not lead or not nonce or lead.get("acceso_nonce") != nonce:
        return None
    return email


async def clave_temporal(email: str) -> Optional[str]:
    """Genera una contraseña nueva para alguien que la perdió."""
    if not lead_get(email):
        return None
    nueva = secrets.token_urlsafe(6)
    await guardar_clave(email, nueva)
    return nueva


def is_guest(user: Optional[str]) -> bool:
    return bool(user) and user.startswith(GUEST_PREFIX)


def guest_email(user: str) -> str:
    return user[len(GUEST_PREFIX):]


def require_user(request: Request) -> str:
    """Protege los endpoints de análisis."""
    user = current_user(request)
    if is_guest(user) and not GUEST_MODE:
        user = None
    if not user:
        raise HTTPException(status_code=401, detail="Tu sesión expiró. Vuelve a entrar.")
    return user


def block_guest(user: str):
    """Fuentes que cuestan dinero aparte y no se abren a invitados."""
    if is_guest(user) and not GUEST_FULL:
        raise HTTPException(status_code=403, detail="La fuente de Facebook está disponible solo para cuentas. Usa YouTube o pega los comentarios.")


SIN_CREDITOS = ("Se te acabaron los créditos. El Radar sigue gratis y sin límite: "
                "puedes seguir extrayendo y ordenando comentarios. "
                "El Analista y el Copy necesitan créditos.")


def check_quota(user: str, modo: str = "rapido"):
    """El Radar es libre. El Analista y el Copy gastan un crédito."""
    if not is_guest(user):
        return
    if COSTO_CREDITOS.get(modo, 1) == 0:
        return
    if creditos_de(guest_email(user)) == 0:
        raise HTTPException(status_code=402, detail=SIN_CREDITOS)


def usos_copy(user: str) -> int:
    """Cuántas piezas de copy lleva este usuario (se cuentan desde el historial)."""
    if not USA_SUPABASE:
        return 0
    filas = _sb_tabla("analisis", "GET", params={
        "usuario": f"eq.{user}", "agente": "eq.copy", "select": "id", "limit": 50,
    })
    return len(filas)


def check_quota_copy(user: str):
    check_quota(user, "copy")


def modelo_para(user: str) -> Optional[str]:
    return GUEST_MODEL if is_guest(user) else None

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
apify_client = ApifyClientAsync(APIFY_API_TOKEN) if APIFY_API_TOKEN else None


# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------
Modo = Literal["rapido", "mercado", "copy"]


class AnalyzeRequest(BaseModel):
    url: HttpUrl
    modo: Modo = "rapido"
    nicho: Optional[str] = Field(None, max_length=150)


class YouTubeSearchRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=150)
    max_videos: int = Field(5, ge=1, le=10)
    modo: Modo = "rapido"
    nicho: Optional[str] = Field(None, max_length=150)


class PasteRequest(BaseModel):
    texto: str = Field(..., min_length=20, max_length=400_000)
    modo: Modo = "mercado"
    nicho: Optional[str] = Field(None, max_length=150)


class Insight(BaseModel):
    tema: str = Field(..., description="Resumen corto del dolor/objeción/deseo")
    frecuencia: str = Field(..., description="alta | media | baja")
    ejemplos: List[str] = Field(..., description="Citas textuales de comentarios")


class AnalysisResult(BaseModel):
    dolores: List[Insight]
    objeciones: List[Insight]
    deseos: List[Insight]
    resumen: str


class VideoInfo(BaseModel):
    titulo: str
    canal: str
    url: str
    comentarios_analizados: int



# --- Agente Analista de Mercado: estructura del informe (8 pasos) ---------
class FraseRepetida(BaseModel):
    frase: str = Field(..., description="Frase exacta o casi exacta que usa la gente")
    frecuencia: str = Field(..., description="alta | media | baja")
    que_revela: str = Field(..., description="Qué dice esta frase sobre su frustración")


class IntentoFallido(BaseModel):
    que_intentaron: str
    por_que_fallo: str = Field(..., description="Por qué no les funcionó, según los comentarios")
    ejemplo: str = Field(..., description="Cita textual que lo muestra")


class ResultadoDeseado(BaseModel):
    lo_que_dicen: str = Field(..., description="El resultado que piden explícitamente")
    lo_que_realmente_buscan: str = Field(..., description="El resultado profundo, aunque no lo digan")
    evidencia: List[str] = Field(..., description="1 a 3 citas textuales")


class DolorEmocional(BaseModel):
    intensidad: int = Field(..., ge=1, le=10, description="1 = molestia leve, 10 = desesperación")
    emociones: List[str] = Field(..., description="Emociones dominantes: vergüenza, miedo, frustración...")
    costo_de_seguir_igual: str = Field(..., description="Qué les duele si en un mes siguen igual")
    evidencia: List[str] = Field(..., description="1 a 3 citas textuales")


class DisposicionPago(BaseModel):
    nivel: str = Field(..., description="alta | media | baja")
    senales: List[str] = Field(..., description="Señales de que pagarían o de que no pagarían")
    en_que_ya_gastan: str = Field(..., description="Productos, cursos o servicios que mencionan haber pagado; o 'No se menciona'")


class BrechaOportunidad(BaseModel):
    descripcion: str = Field(..., description="Lo que nadie está resolviendo directamente")
    por_que_no_esta_resuelto: str
    evidencia: List[str] = Field(..., description="1 a 3 citas textuales")


class ProblemaUrgente(BaseModel):
    problema_macro_a_evitar: str = Field(..., description="El problema general del nicho (título de curso de 8 módulos)")
    problema_urgente_especifico: str = Field(..., description="Una situación concreta, en palabras del cliente")
    por_que_es_especifico: str = Field(..., description="Por qué no podría ser el título de un curso de 8 módulos")


class PropuestaProducto(BaseModel):
    nombre: str = Field(..., description="Nombre del producto con el mecanismo único integrado")
    mecanismo_unico: str = Field(..., description="El cómo diferente, diseñado a partir de lo que ya falló")
    promesa: str = Field(..., description="Resultado específico y creíble, en una frase")
    formato: str = Field(..., description="Plantilla, checklist, mini curso, taller, guion, etc.")
    que_incluye: List[str]
    precio_sugerido_usd: float
    justificacion_precio: str


class Veredicto(BaseModel):
    recomendacion: str = Field(..., description="CREAR | VALIDAR MÁS | NO CREAR")
    puntuacion: int = Field(..., ge=1, le=10, description="Qué tan buena es la oportunidad")
    justificacion: str
    riesgos: List[str]
    siguiente_paso: str = Field(..., description="La acción concreta para validar en 7 días o menos")


class MarketReport(BaseModel):
    resumen_ejecutivo: str
    frases_repetidas: List[FraseRepetida]
    intentos_fallidos: List[IntentoFallido]
    resultado_deseado: ResultadoDeseado
    dolor_emocional: DolorEmocional
    disposicion_a_pagar: DisposicionPago
    brecha_oportunidad: BrechaOportunidad
    problema_urgente: ProblemaUrgente
    propuesta_producto: PropuestaProducto
    veredicto: Veredicto


# --- Agente Copy ----------------------------------------------------------
FORMATOS_COPY = {
    "anuncio": "Anuncio para Meta Ads",
    "email": "Email",
    "reel": "Guion de reel (30-60 s)",
    "post": "Post individual (Facebook / Instagram)",
    "hilo": "Hilo (Facebook / Threads)",
}
ANGULOS_A = {"dolor": "Dolor", "deseo": "Deseo", "objecion": "Objeción"}
ANGULOS_B = {
    "storytelling": "Storytelling",
    "caso": "Caso de estudio",
    "framework": "Framework de 3 pasos",
    "desmitificador": "El Desmitificador",
    "autopsia": "La Autopsia del Error",
    "contraintuitivo": "El Contra-Intuitivo",
}
ESTADOS_PRODUCTO = {
    "idea": "Todavía es una idea (lista de espera)",
    "preventa": "En preventa",
    "listo": "Ya está listo para vender",
}


class BloqueCopy(BaseModel):
    etiqueta: str = Field(..., description="Nombre del bloque: Texto principal, Titular, Asunto, Gancho (0-3 s)...")
    texto: str = Field(..., description="El contenido listo para copiar y pegar")
    nota: Optional[str] = Field(None, description="Indicación breve: duración, texto en pantalla, límite de caracteres")


class FraseUsada(BaseModel):
    frase: str = Field(..., description="La frase textual del mercado")
    uso: str = Field(..., description="Cómo se usó en la pieza")


class PiezaCopy(BaseModel):
    formato: str = Field(..., description="anuncio | email | reel | post | hilo")
    angulo: str = Field(..., description="Capa A + Capa B, por ejemplo: Dolor + Storytelling")
    bloques: List[BloqueCopy]
    frases_usadas: List[FraseUsada]
    ajuste_meta: Optional[str] = Field(None, description="Qué se reescribió para cumplir políticas de Meta, si aplicó")


class CopyResult(BaseModel):
    piezas: List[PiezaCopy]
    prueba_sugerida: str = Field(..., description="Qué pieza publicar primero y por qué, en una sola oración")


class CopyRequest(BaseModel):
    analisis_id: str
    formato: str = "post"
    angulo_a: str = "dolor"
    angulo_b: str = "storytelling"
    estado: str = "idea"
    paquete: bool = False


class AnalyzeResponse(BaseModel):
    fuente: str
    consulta: str
    total_comentarios: int
    modelo: str
    modo: Modo
    analisis: Optional[AnalysisResult] = None
    mercado: Optional[MarketReport] = None
    videos: List[VideoInfo] = []
    restantes: Optional[int] = None   # análisis de prueba que le quedan al invitado
    comentarios: List[str] = []       # texto crudo extraído (para copiar o pasar al Analista)
    copywriting: Optional[CopyResult] = None
    id_analisis: Optional[str] = None   # para encadenar con el agente Copy



def _clean(texts: List[str]) -> List[str]:
    out = []
    for t in texts:
        if isinstance(t, str) and t.strip():
            out.append(t.strip()[:MAX_COMMENT_CHARS])
    return out


# ---------------------------------------------------------------------------
# Facebook (Apify)
# ---------------------------------------------------------------------------
def _field(obj, attr: str, key: str):
    """Lee un campo tanto si Apify devuelve un dict como un objeto."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, attr, None)


async def fetch_facebook_comments(url: str) -> List[str]:
    if apify_client is None:
        raise HTTPException(status_code=500, detail="Falta la variable APIFY_API_TOKEN.")

    run_input = {
        "startUrls": [{"url": url}],
        "resultsLimit": MAX_COMMENTS,
        "includeNestedComments": False,
        "viewOption": "RANKED_UNFILTERED",
    }

    try:
        run = await apify_client.actor(APIFY_ACTOR_ID).call(
            run_input=run_input,
            wait_duration=timedelta(minutes=4),
        )
    except Exception as exc:
        logger.exception("Error llamando a Apify")
        raise HTTPException(status_code=502, detail=f"Error al ejecutar Apify: {exc}") from exc

    status = _field(run, "status", "status")
    status = getattr(status, "value", status)
    if not run or str(status) != "SUCCEEDED":
        raise HTTPException(
            status_code=502,
            detail=f"El scraper de Apify no terminó bien (estado: {status or 'desconocido'}).",
        )

    dataset_id = _field(run, "default_dataset_id", "defaultDatasetId")
    page = await apify_client.dataset(dataset_id).list_items()
    items = page.items or []
    comments = _clean([item.get("text") for item in items if isinstance(item, dict)])
    logger.info("Facebook: %d comentarios", len(comments))
    return comments


# ---------------------------------------------------------------------------
# YouTube (API oficial)
# ---------------------------------------------------------------------------
YT_ID_PATTERNS = [
    r"(?:v=|/videos/|/embed/|/shorts/|/live/|youtu\.be/)([A-Za-z0-9_-]{11})",
]


def extract_video_id(url: str) -> Optional[str]:
    for pattern in YT_ID_PATTERNS:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def _yt_error(resp: httpx.Response):
    """Devuelve (reason, message) del error de Google."""
    try:
        err = resp.json()["error"]
        first = (err.get("errors") or [{}])[0]
        return first.get("reason", ""), err.get("message", "")
    except Exception:
        return "", resp.text[:200]


async def yt_get(client: httpx.AsyncClient, path: str, params: dict) -> Optional[dict]:
    """Llama a la API de YouTube. En commentThreads devuelve None si el video no permite leer comentarios."""
    params = {**params, "key": YOUTUBE_API_KEY.strip()}
    try:
        resp = await client.get(f"{YT_API}/{path}", params=params)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"No se pudo conectar con YouTube: {exc}") from exc

    if resp.status_code == 200:
        return resp.json()

    reason, message = _yt_error(resp)
    logger.warning("YouTube %s -> %s %s: %s", path, resp.status_code, reason, message)

    if path == "commentThreads" and reason in ("commentsDisabled", "videoNotFound", "forbidden"):
        return None
    if reason in ("quotaExceeded", "dailyLimitExceeded"):
        raise HTTPException(status_code=429, detail="Se acabó la cuota diaria gratis de YouTube. Vuelve a intentarlo mañana.")
    if "API key not valid" in message or reason == "keyInvalid":
        raise HTTPException(status_code=502, detail="La clave de YouTube no es válida. Revisa que YOUTUBE_API_KEY esté copiada completa, sin espacios ni comillas.")
    if reason in ("accessNotConfigured", "SERVICE_DISABLED") or "has not been used" in message or "is disabled" in message:
        raise HTTPException(status_code=502, detail="La YouTube Data API v3 no está habilitada en el proyecto de Google de esa clave.")
    if "blocked" in message.lower() or reason in ("forbidden", "API_KEY_SERVICE_BLOCKED"):
        raise HTTPException(status_code=502, detail="Esa clave tiene restricciones que no permiten usar YouTube. Crea una clave nueva sin restricciones o permite YouTube Data API v3.")
    raise HTTPException(status_code=502, detail=f"Error de YouTube ({resp.status_code} {reason}): {message}")


async def fetch_video_comments(client: httpx.AsyncClient, video_id: str) -> List[str]:
    comments: List[str] = []
    page_token = None
    while len(comments) < YT_COMMENTS_PER_VIDEO:
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": min(100, YT_COMMENTS_PER_VIDEO - len(comments)),
            "order": "relevance",
            "textFormat": "plainText",
        }
        if page_token:
            params["pageToken"] = page_token
        data = await yt_get(client, "commentThreads", params)
        if not data:
            break
        for item in data.get("items", []):
            text = item["snippet"]["topLevelComment"]["snippet"].get("textDisplay", "")
            comments.append(text)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return _clean(comments)


async def get_video_details(client: httpx.AsyncClient, video_ids: List[str]) -> List[dict]:
    data = await yt_get(client, "videos", {"part": "snippet,statistics", "id": ",".join(video_ids)})
    videos = []
    for item in (data or {}).get("items", []):
        videos.append({
            "id": item["id"],
            "titulo": item["snippet"].get("title", ""),
            "canal": item["snippet"].get("channelTitle", ""),
            "comment_count": int(item.get("statistics", {}).get("commentCount", 0) or 0),
        })
    return videos


async def collect_youtube(client: httpx.AsyncClient, videos: List[dict]):
    results = await asyncio.gather(*(fetch_video_comments(client, v["id"]) for v in videos))
    all_comments: List[str] = []
    info: List[VideoInfo] = []
    for video, comments in zip(videos, results):
        if not comments:
            continue
        all_comments.extend(comments)
        info.append(VideoInfo(
            titulo=video["titulo"],
            canal=video["canal"],
            url=f"https://www.youtube.com/watch?v={video['id']}",
            comentarios_analizados=len(comments),
        ))
    return all_comments, info


def _require_youtube_key():
    if not YOUTUBE_API_KEY:
        raise HTTPException(status_code=500, detail="Falta la variable YOUTUBE_API_KEY.")



# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------
QUICK_PROMPT = """Eres un estratega de marketing experto en investigación de mercado y copywriting de respuesta directa.
Analizas comentarios reales de clientes en redes sociales para extraer:
- DOLORES: problemas, frustraciones o miedos que expresan.
- OBJECIONES: dudas, desconfianzas o razones por las que no comprarían (precio, tiempo, credibilidad, etc.).
- DESEOS: resultados, aspiraciones o transformaciones que quieren lograr.

Reglas:
- Agrupa ideas repetidas en un solo tema y estima su frecuencia (alta, media, baja).
- Incluye 1 a 3 citas textuales cortas como ejemplos por tema, copiadas tal cual de los comentarios.
- Ignora spam, saludos vacíos y comentarios que solo halagan al creador sin decir nada del tema.
- No inventes nada que no esté en los comentarios. Si una categoría no aparece, devuélvela vacía.
- Escribe en el mismo idioma predominante de los comentarios.
- El resumen debe ser de 2 a 4 frases con la conclusión accionable para marketing."""


MARKET_PROMPT = """Eres el AGENTE ANALISTA DE MERCADO. Tu trabajo es decidir, con evidencia, si existe un problema real que vale la pena resolver con un producto, antes de que alguien invierta tiempo en crearlo.
Recibes comentarios reales de redes sociales. Los conviertes en inteligencia de mercado accionable siguiendo 8 pasos, en este orden:

1. FRASES REPETIDAS: detecta las frases exactas que se repiten, las palabras que la gente usa cuando describe su frustración. Cópialas como las escriben (con su ortografía y jerga); no las parafrasees.
2. INTENTOS FALLIDOS: identifica qué ya intentaron y no les funcionó, y por qué. Es la materia prima del mecanismo único.
3. RESULTADO DESEADO: distingue lo que dicen que quieren del resultado que realmente buscan, aunque no lo digan directamente.
4. DOLOR EMOCIONAL: evalúa la intensidad (1 a 10), las emociones dominantes y qué les duele si dentro de un mes siguen igual.
5. DISPOSICIÓN A PAGAR: busca señales reales (ya pagaron cursos, contrataron a alguien, piden recomendaciones de herramientas, dicen "pagaría por…", o al revés: "todo está gratis en YouTube"). No la supongas.
6. BRECHA DE OPORTUNIDAD: detecta lo que nadie está resolviendo directamente, según lo que los comentarios reclaman y no encuentran.
7. PROPUESTA DE PRODUCTO: propone un producto con su mecanismo único integrado en el nombre. El mecanismo debe salir de los pasos 2 y 6: explica por qué esto funcionará donde lo anterior falló.
8. VEREDICTO: recomienda CREAR, VALIDAR MÁS o NO CREAR, con puntuación de 1 a 10, riesgos y un siguiente paso concreto para validar en 7 días o menos.

LA REGLA MÁS IMPORTANTE: el problema urgente específico.
Nunca resuelvas el problema macro del nicho. Siempre resuelve un problema urgente y específico dentro de ese nicho.
- Macro (evitar): "Cómo vender más". Específico (atacar): "Llego al cierre y el cliente me dice 'lo pienso' y desaparece".
- Macro: "Cómo bajar de peso". Específico: "Empiezo la dieta los lunes y la abandono el miércoles".
- Macro: "Cómo salvar mi matrimonio". Específico: "Mi pareja y yo peleamos cada vez que hablamos de dinero".
Prueba: si el problema podría ser el título de un curso de 8 módulos en Hotmart, es macro. Ve más profundo. El problema urgente específico debe describir una situación concreta, en palabras del cliente, y el producto propuesto debe resolver SOLO ese problema.

Reglas de evidencia y honestidad:
- Todo debe apoyarse en los comentarios. Las citas van textuales. No inventes datos, cifras ni testimonios.
- Si la evidencia es débil (pocos comentarios útiles, dolor bajo, sin señales de pago), dilo y baja la puntuación. Un "NO CREAR" o "VALIDAR MÁS" bien justificado vale más que un "CREAR" optimista.
- Si una lista no tiene evidencia, devuélvela vacía en lugar de rellenarla.
- El precio sugerido en USD debe ser coherente con el formato, la intensidad del dolor y las señales de pago. Justifícalo.
- Ignora spam, saludos y halagos vacíos al creador.
- Escribe en el idioma predominante de los comentarios, claro y directo, para un emprendedor que va a tomar una decisión."""


COPY_PROMPT = """Eres COPY, el agente de TΛLENO OS que convierte el informe del Analista de Mercado en contenido de venta listo para publicar. Escribes con las palabras reales del mercado, no con frases genéricas de marketing. Tu trabajo cierra el ciclo: escuchar, decidir, vender.

USA LAS FRASES DEL MERCADO
- Cada pieza debe usar al menos una frase textual del informe (literal o adaptada).
- Devuelve siempre las frases que usaste y cómo las usaste.
- El problema urgente específico y el mecanismo único son los mismos en todas las piezas: lo que cambia es el ángulo.
- Nunca inventes detalles del producto que no estén en el informe: ni bonos, ni módulos, ni plazos, ni cifras.
- Si el estado del producto es "idea" o "preventa", escribe como lista de espera o preventa, nunca como si ya se pudiera comprar hoy.

ÁNGULOS
Capa A (qué ataca): Dolor (lo que le duele hoy), Deseo (lo que quiere lograr), Objeción (lo que le impide comprar).
Capa B (cómo lo cuenta): Storytelling (historia con conflicto y giro), Caso de estudio (problema, acción, resultado, sin inventar datos), Framework de 3 pasos, El Desmitificador (derriba una creencia falsa), La Autopsia del Error (el error más común y su costo), El Contra-Intuitivo (una idea que va contra lo que todos creen).
Si te piden varias piezas, no repitas la misma combinación de ángulos en dos piezas.

FORMATOS Y BLOQUES
- anuncio: bloques "Texto principal" (la primera línea es el gancho y debe funcionar sola; 60-150 palabras), "Titular" (unos 40 caracteres), "Descripción" (unos 30 caracteres) y "Botón sugerido" (Más información / Registrarte / Comprar).
- email: bloques "Asunto" (hasta 50 caracteres, sin mayúsculas completas ni signos excesivos), "Preencabezado" (hasta 90 caracteres, complementa el asunto) y "Cuerpo" (120-250 palabras, párrafos de 1 a 3 líneas, una sola llamada a la acción).
- reel: bloques en orden "Gancho (0-3 s)", "Problema", "Razón", "Solución", "CTA". En cada uno, el texto es lo que se dice, y la nota indica el texto en pantalla y la duración aproximada.
- post: bloques "Gancho", "Cuerpo" (80-200 palabras, líneas cortas) y "Cierre" (una pregunta abierta real).
- hilo: bloques "Post inicial" (gancho más promesa del hilo), luego "Comentario 1" a "Comentario 4" o "Comentario 5" (una idea por comentario, cada uno entendible por sí solo) y "Comentario final" (conclusión y llamada a la acción).
Los límites de caracteres son guías de referencia, no reglas exactas: las plataformas los cambian seguido.

FILTRO DE POLÍTICAS DE META (obligatorio)
Antes de entregar, revisa cada pieza y reescribe lo que incumpla:
1. Atributos personales: no afirmes ni insinúes características del lector (salud, finanzas, edad, situación personal). Mal: "¿Tú no sabes vender?". Bien: "Muchos emprendedores no saben por dónde empezar a vender". Las frases del mercado en primera persona se adaptan a tercera persona o a lenguaje general.
2. Resultados y dinero: no prometas ingresos, cifras ni resultados garantizados.
3. Antes y después: no uses comparaciones exageradas de transformación.
4. Urgencia falsa: no inventes escasez ni plazos que no existan.
5. Interacción forzada en contenido orgánico: nada de "comenta SÍ", "etiqueta a 3 amigos" ni "dale like si...". Usa preguntas reales.
6. Sin testimonios inventados ni citas de personas reales.
Si reescribiste algo por esta razón, dilo en una línea en el campo de ajuste.

TONO
Español claro y neutro, tuteo. Directo y cercano, sin tecnicismos ni frases vacías como "revoluciona tu vida" o "el secreto que nadie te cuenta". Frases cortas, una idea por párrafo.

Cierra con una sola oración indicando qué pieza publicar primero y por qué."""


RETRYABLE = {429, 500, 503, 504}


async def _gemini_json(system: str, user_message: str, schema, modelo: Optional[str] = None):
    """Llama a Gemini con reintentos. Si el modelo principal está saturado, prueba el de respaldo."""
    if gemini_client is None:
        raise HTTPException(status_code=500, detail="Falta la variable GEMINI_API_KEY.")

    config = types.GenerateContentConfig(
        system_instruction=system,
        response_mime_type="application/json",
        response_schema=schema,
        temperature=0.3,
    )
    principal = modelo or GEMINI_MODEL
    models = [principal] + ([GEMINI_FALLBACK_MODEL] if GEMINI_FALLBACK_MODEL and GEMINI_FALLBACK_MODEL != principal else [])
    delays = [0, 3, 8]  # segundos de espera antes de cada intento
    last_exc = None

    for model in models:
        for delay in delays:
            if delay:
                await asyncio.sleep(delay)
            try:
                response = await gemini_client.aio.models.generate_content(
                    model=model, contents=user_message, config=config,
                )
            except genai_errors.APIError as exc:
                last_exc = exc
                code = getattr(exc, "code", None)
                logger.warning("Gemini %s falló (%s). Reintentando...", model, code)
                if code in RETRYABLE:
                    continue
                if code == 404:
                    break  # modelo no disponible: pasa al de respaldo
                raise HTTPException(status_code=502, detail=f"Error en la API de Gemini: {exc}") from exc

            if model != principal:
                logger.info("Respuesta obtenida con el modelo de respaldo %s", model)
            if isinstance(response.parsed, schema):
                return response.parsed
            try:
                return schema.model_validate_json(response.text or "")
            except Exception as exc:
                last_exc = exc
                logger.warning("JSON inválido de %s, reintentando", model)
                continue

    code = getattr(last_exc, "code", None)
    if code in RETRYABLE:
        raise HTTPException(
            status_code=503,
            detail="Gemini está saturado en este momento (mucha demanda). Espera un par de minutos y vuelve a intentarlo.",
        )
    raise HTTPException(status_code=502, detail=f"Error en la API de Gemini: {last_exc}")


async def run_analysis(comments: List[str], contexto: str, modo: str, nicho: Optional[str], modelo: Optional[str] = None) -> dict:
    numbered = "\n".join(f"{i + 1}. {c}" for i, c in enumerate(comments))
    nicho_txt = f"\nNicho o tema: {nicho.strip()}" if nicho and nicho.strip() else ""
    user_message = f"Fuente: {contexto}.{nicho_txt}\nTotal de comentarios: {len(comments)}\n\nCOMENTARIOS:\n{numbered}"

    if modo == "mercado":
        return {"mercado": await _gemini_json(MARKET_PROMPT, user_message, MarketReport, modelo)}
    return {"analisis": await _gemini_json(QUICK_PROMPT, user_message, AnalysisResult, modelo)}


def resumen_para_copy(datos: dict) -> str:
    """Convierte el informe guardado en el texto que recibe Copy."""
    m = datos.get("mercado") or {}
    p = m.get("propuesta_producto", {})
    pu = m.get("problema_urgente", {})
    v = m.get("veredicto", {})
    partes = [
        f"PROBLEMA URGENTE ESPECÍFICO: {pu.get('problema_urgente_especifico', '')}",
        f"(problema macro a evitar: {pu.get('problema_macro_a_evitar', '')})",
        f"PRODUCTO: {p.get('nombre', '')}",
        f"MECANISMO ÚNICO: {p.get('mecanismo_unico', '')}",
        f"PROMESA: {p.get('promesa', '')}",
        f"FORMATO: {p.get('formato', '')}",
        f"INCLUYE: {', '.join(p.get('que_incluye') or [])}",
        f"PRECIO SUGERIDO: US$ {p.get('precio_sugerido_usd', '')}",
        f"VEREDICTO: {v.get('recomendacion', '')} ({v.get('puntuacion', '')}/10)",
        f"RESUMEN: {m.get('resumen_ejecutivo', '')}",
        "",
        "FRASES TEXTUALES QUE SE REPITEN:",
    ]
    for f in (m.get("frases_repetidas") or []):
        partes.append(f"- \"{f.get('frase','')}\" ({f.get('frecuencia','')}): {f.get('que_revela','')}")
    partes.append("\nLO QUE YA INTENTARON Y NO FUNCIONÓ:")
    for i in (m.get("intentos_fallidos") or []):
        partes.append(f"- {i.get('que_intentaron','')}: {i.get('por_que_fallo','')} | cita: \"{i.get('ejemplo','')}\"")
    rd = m.get("resultado_deseado", {})
    partes.append(f"\nRESULTADO QUE BUSCAN: dicen \"{rd.get('lo_que_dicen','')}\"; en realidad buscan {rd.get('lo_que_realmente_buscan','')}")
    for cita in (rd.get("evidencia") or []):
        partes.append(f"- cita: \"{cita}\"")
    de = m.get("dolor_emocional", {})
    partes.append(f"\nDOLOR EMOCIONAL: {de.get('intensidad','')}/10 ({', '.join(de.get('emociones') or [])}). {de.get('costo_de_seguir_igual','')}")
    for cita in (de.get("evidencia") or []):
        partes.append(f"- cita: \"{cita}\"")
    dp = m.get("disposicion_a_pagar", {})
    partes.append(f"\nDISPOSICIÓN A PAGAR: {dp.get('nivel','')}. Señales: {'; '.join(dp.get('senales') or [])}")
    bo = m.get("brecha_oportunidad", {})
    partes.append(f"\nBRECHA: {bo.get('descripcion','')} — {bo.get('por_que_no_esta_resuelto','')}")
    return "\n".join(str(x) for x in partes)


async def run_copy(datos: dict, formato: str, angulo_a: str, angulo_b: str,
                   estado: str, paquete: bool, modelo: Optional[str] = None) -> CopyResult:
    informe = resumen_para_copy(datos)
    estado_txt = ESTADOS_PRODUCTO.get(estado, ESTADOS_PRODUCTO["idea"])
    if paquete:
        pedido = ("Escribe UNA pieza de cada formato: anuncio, email, reel, post e hilo. "
                  "Usa un ángulo distinto en cada una, empezando por "
                  f"{ANGULOS_A.get(angulo_a, 'Dolor')} + {ANGULOS_B.get(angulo_b, 'Storytelling')}.")
    else:
        pedido = (f"Escribe UNA sola pieza en formato {formato} ({FORMATOS_COPY.get(formato, formato)}), "
                  f"con el ángulo {ANGULOS_A.get(angulo_a, 'Dolor')} + {ANGULOS_B.get(angulo_b, 'Storytelling')}.")

    mensaje = (f"ESTADO DEL PRODUCTO: {estado_txt}\n\n{pedido}\n\n"
               f"INFORME DEL ANALISTA DE MERCADO:\n{informe}")
    return await _gemini_json(COPY_PROMPT, mensaje, CopyResult, modelo)


def parse_pasted(texto: str) -> List[str]:
    """Convierte el texto pegado en una lista de comentarios (uno por línea)."""
    lines = []
    for raw in texto.splitlines():
        line = re.sub(r"^\s*(\d+[\.\)\-:]|[-•*·])\s*", "", raw).strip()
        if len(line) >= 3:
            lines.append(line)
    return _clean(lines)[:MAX_PASTED]


NO_COMMENTS = "No se encontraron comentarios. Verifica que la publicación sea pública y tenga comentarios activados."



# ---------------------------------------------------------------------------
# Interfaz
# ---------------------------------------------------------------------------
CEREBROS = {
    "radar": {
        "nombre": "Radar",
        "modo": "rapido",
        "lema": "Escucha lo que tu mercado ya está diciendo",
        "desc": "Extrae comentarios de YouTube o Facebook y los ordena en dolores, objeciones y deseos. Al final puedes copiarlos todos o mandarlos al Analista.",
        "tiempo": "30 s a 3 min",
        "fuentes": ["yt-search", "yt-video", "facebook", "paste"],
    },
    "analista": {
        "nombre": "Analista",
        "modo": "analista",
        "lema": "Del dolor al producto, en 8 pasos",
        "desc": "Pega aquí los comentarios (o tráelos desde el Radar). Detecta el problema urgente específico, propone el producto con su mecanismo único y da un veredicto: crear o no crear.",
        "tiempo": "1 a 4 min",
        "fuentes": ["paste"],
    },
    "copy": {
        "nombre": "Copy",
        "modo": "copy",
        "lema": "Del informe al contenido que vende",
        "desc": "Toma un análisis del Analista y escribe el contenido con las palabras reales de tu mercado: anuncio, email, guion de reel, post o hilo. Revisa políticas de Meta antes de entregar.",
        "tiempo": "20 s a 1 min",
        "fuentes": ["informe"],
    },
}

BASE_CSS = """
  :root {
    --naranja: #ff6b2b; --azul: #3b82f6; --tinta: #0b0b0f; --gris: #6b7280;
    --linea: #e6e8ec; --papel: #ffffff; --fondo: #f6f7f9;
    --dolor: #c2410c; --objecion: #9a6700; --deseo: #0f766e;
    --si: #0f766e; --tal: #9a6700; --no: #b42318;
    --barra-h: 60px; --lateral-w: 250px; --lateral-min: 64px;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--fondo); color: var(--tinta);
    font: 16px/1.5 "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased; -webkit-text-size-adjust: 100%; }
  h1, h2, h3, .marca, .rec, .nombre { font-weight: 700; letter-spacing: -0.015em; }
  a { color: inherit; }

  /* ---- barra superior ---- */
  .topbar { position: fixed; top: 0; left: 0; right: 0; height: var(--barra-h); z-index: 30;
    background: var(--papel); border-bottom: 1px solid var(--linea);
    display: flex; align-items: center; gap: 12px; padding: 0 16px; }
  .marca { font-size: 17px; font-weight: 700; text-decoration: none; white-space: nowrap; }
  .marca b { color: var(--naranja); }
  .topbar .sp { flex: 1; }
  .hamb { display: inline-flex; align-items: center; justify-content: center; width: 40px; height: 40px;
    border: 1px solid var(--linea); border-radius: 10px; background: var(--papel); cursor: pointer; font-size: 18px; }
  .usuario { display: flex; align-items: center; gap: 10px; }
  .avatar { width: 32px; height: 32px; border-radius: 50%; background: var(--tinta); color: #fff;
    display: grid; place-items: center; font-size: 13px; font-weight: 700; }
  .nombre-usuario { font-size: 14px; font-weight: 600; max-width: 150px; overflow: hidden;
    text-overflow: ellipsis; white-space: nowrap; }
  .btn-sesion { font: 600 14px inherit; text-decoration: none; padding: 9px 16px;
    border-radius: 10px; border: 1.5px solid var(--linea); color: var(--gris); background: var(--papel); }
  .btn-sesion.primario { background: var(--naranja); border-color: var(--naranja); color: #fff; }
  .chip.compra { text-decoration: none; }
  .chip { font-size: 12px; font-weight: 700; color: var(--naranja); background: #fff1e9;
    border-radius: 999px; padding: 5px 10px; white-space: nowrap; }

  /* ---- barra lateral ---- */
  .lateral { position: fixed; top: var(--barra-h); bottom: 0; left: 0; width: var(--lateral-w); z-index: 20;
    background: var(--papel); border-right: 1px solid var(--linea); padding: 16px 12px;
    display: flex; flex-direction: column; gap: 4px; overflow-y: auto; transition: width .18s, transform .18s; }
  .ficha { display: flex; align-items: center; gap: 10px; padding: 8px 12px 14px;
    border-bottom: 1px solid var(--linea); margin-bottom: 8px; }
  .ficha .datos { display: flex; flex-direction: column; overflow: hidden; }
  .ficha .correo { font-size: 12px; color: var(--gris); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  body.mini .ficha { justify-content: center; padding: 8px 0 14px; }
  body.mini .ficha .datos { display: none; }
  .salir-nav { margin-top: 8px; color: var(--gris); }
  .salir-nav:hover { color: var(--no); }
  .grupo { font-size: 11px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase;
    color: var(--gris); padding: 14px 12px 6px; }
  .nav { display: flex; align-items: center; gap: 12px; padding: 11px 12px; border-radius: 10px;
    text-decoration: none; color: var(--tinta); font-size: 15px; font-weight: 600; }
  .nav:hover { background: var(--fondo); }
  .nav[aria-current="page"] { background: #fff1e9; color: #b8430f; }
  .nav .ic { width: 22px; text-align: center; font-size: 16px; flex: none; }
  .nav .tx { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .nav.mudo { color: var(--gris); font-weight: 400; cursor: default; }
  .punto { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    background: var(--naranja); margin-left: 6px; }
  .empuje { flex: 1; }
  .pie-lateral { margin-top: 0; padding: 14px 12px; font-size: 12px; color: var(--gris);
    display: flex; flex-direction: column; gap: 4px; }
  .pie-lateral a { color: var(--gris); text-decoration: none; word-break: break-all; }
  .pie-lateral a:hover { color: var(--naranja); }
  .pie-lateral .tele { color: var(--azul); font-weight: 600; margin-bottom: 6px; }

  /* colapsada (escritorio) */
  body.mini .lateral { width: var(--lateral-min); padding-left: 8px; padding-right: 8px; }
  body.mini .lateral .tx, body.mini .grupo, body.mini .pie-lateral { display: none; }
  body.mini .nav { justify-content: center; padding: 11px 0; }
  body.mini .contenido { margin-left: var(--lateral-min); }

  /* ---- contenido ---- */
  .contenido { margin-top: var(--barra-h); margin-left: var(--lateral-w); padding: 28px 28px 80px;
    max-width: 1100px; transition: margin-left .18s; }
  .contenido.solo { margin-left: 0; }
  .velo { display: none; position: fixed; inset: var(--barra-h) 0 0 0; background: rgba(11,11,15,.35); z-index: 15; }

  @media (max-width: 860px) {
    .lateral { transform: translateX(-100%); width: 78%; max-width: 280px; }
    body.abierta .lateral { transform: none; }
    body.abierta .velo { display: block; }
    .contenido, body.mini .contenido { margin-left: 0; padding: 20px 16px 72px; }
  }

  h1 { font-size: clamp(24px, 5vw, 34px); line-height: 1.14; margin: 0 0 10px; }
  .intro { color: var(--gris); margin: 0 0 22px; max-width: 62ch; }
  .label { font-size: 12px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; color: var(--gris); margin: 20px 0 8px; }
  .tabs { display: flex; gap: 8px; flex-wrap: wrap; }
  .tab { padding: 10px 15px; font: 600 14px inherit; color: var(--gris);
    background: var(--papel); border: 1.5px solid var(--linea); border-radius: 999px; cursor: pointer; }
  .tab[aria-pressed="true"] { color: #fff; background: var(--tinta); border-color: var(--tinta); }
  .hint { color: var(--gris); font-size: 14px; margin: 14px 0 10px; }
  input, select, textarea { width: 100%; padding: 14px 16px; font: inherit; color: var(--tinta);
    background: var(--papel); border: 1.5px solid var(--linea); border-radius: 12px; }
  textarea { min-height: 200px; resize: vertical; font-size: 15px; }
  input:focus, select:focus, textarea:focus { outline: 3px solid rgba(255,107,43,.22); border-color: var(--naranja); }
  .row { display: flex; gap: 10px; flex-wrap: wrap; }
  .row > * { flex: 1 1 220px; }
  .counter { font-size: 13px; color: var(--gris); margin-top: 6px; }
  .submit { width: 100%; margin-top: 16px; padding: 16px 22px; font: 700 16px inherit;
    color: #fff; background: var(--naranja); border: 0; border-radius: 12px; cursor: pointer; }
  .submit:disabled { opacity: .6; cursor: wait; }
  button:focus-visible, a:focus-visible { outline: 3px solid rgba(255,107,43,.4); outline-offset: 2px; }
  [hidden] { display: none !important; }
  .status { margin: 16px 0 0; color: var(--gris); min-height: 1.5em; }
  .status.error { color: var(--no); font-weight: 600; }
  .status.aviso { background: #fff1e9; border: 1px solid #ffd3bd; color: #8a3b12;
    padding: 14px 16px; border-radius: 12px; }
  .tab.bloqueada { opacity: .75; border-style: dashed; }
  .spinner { display: inline-block; width: 14px; height: 14px; margin-right: 8px; vertical-align: -2px;
    border: 2px solid var(--linea); border-top-color: var(--naranja); border-radius: 50%; animation: giro .8s linear infinite; }
  @keyframes giro { to { transform: rotate(360deg); } }
  @media (prefers-reduced-motion: reduce) { .spinner, .lateral, .contenido { animation: none; transition: none; } }
  @media (min-width: 720px) { .submit { width: auto; } }

  /* ---- impresión / guardar como PDF ---- */
  .pie-impresion { display: none; }
  @media print {
    @page { margin: 14mm; }
    body { background: #fff; }
    .topbar, .lateral, .velo, .no-print, form, #fbBox, .barra-detalle,
    .hint, .label, .tabs, .status, .crudos, .extraidos .acciones, .recuperado { display: none !important; }
    .contenido, body.mini .contenido { margin: 0 !important; padding: 0 !important; max-width: none; }
    .pie-impresion { display: block; margin-top: 24px; padding-top: 10px;
      border-top: 1px solid #ddd; font-size: 11px; color: #666; }
    .item, .summary, .crudos, .sources a { break-inside: avoid; page-break-inside: avoid;
      border-color: #ddd !important; box-shadow: none; }
    section { break-inside: auto; margin-top: 18px; }
    h1 { font-size: 24px; }
    h2 { font-size: 17px; }
    .verdict { color: #000 !important; background: #fff !important;
      border: 2px solid #333; break-inside: avoid; }
    .verdict .score { background: #eee !important; color: #000 !important; }
    blockquote { color: #333; }
    a { text-decoration: none; color: #000; }
    .sources a::after { content: " (" attr(href) ")"; font-size: 10px; color: #666; }
  }
"""

RESULT_CSS = """
  .summary { margin: 32px 0 8px; padding: 22px 24px; background: var(--papel);
    border-left: 5px solid var(--naranja); border-radius: 4px 14px 14px 4px; }
  .summary p { margin: 0; font-size: 18px; }
  .meta { color: var(--gris); font-size: 14px; margin-top: 10px; }
  section { margin-top: 32px; }
  h2 { font-size: 20px; margin: 0 0 14px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  h2 .dot { width: 12px; height: 12px; border-radius: 50%; background: var(--c); }
  h2 .step { font-size: 12px; font-weight: 700; color: #fff; background: var(--tinta); border-radius: 6px; padding: 3px 9px; }
  .count { font-weight: 400; color: var(--gris); font-size: 15px; }
  .item { background: var(--papel); border-radius: 14px; padding: 18px 20px; margin-bottom: 12px; border: 1px solid var(--linea); }
  .item-head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; flex-wrap: wrap; }
  .item h3 { margin: 0; font-size: 17px; }
  .item p { margin: 8px 0 0; }
  .freq { font-size: 13px; font-weight: 600; color: var(--c, var(--azul)); white-space: nowrap; }
  blockquote { margin: 10px 0 0; padding: 12px 14px; border-left: 3px solid var(--c, var(--azul));
    background: #f7f8fa; border-radius: 0 10px 10px 0; font-size: 15px; color: #3c4451; }
  blockquote + blockquote { margin-top: 8px; }
  .empty { color: var(--gris); font-style: italic; margin: 0; }
  .verdict { margin-top: 26px; padding: 24px; border-radius: 16px; color: #fff; background: var(--v); }
  .verdict .top { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }
  .verdict .rec { font-size: 24px; font-weight: 700; }
  .verdict .score { font-size: 14px; font-weight: 600; background: rgba(255,255,255,.2); padding: 6px 12px; border-radius: 999px; }
  .verdict p { margin: 12px 0 0; }
  .verdict .next { margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(255,255,255,.3); }
  .problem .macro { text-decoration: line-through; color: var(--gris); }
  .problem .specific { font-size: 19px; font-weight: 600; line-height: 1.35; margin: 10px 0 0; }
  .product { border: 2px solid var(--tinta); }
  .product .nombre { font-size: 22px; font-weight: 700; margin: 0; line-height: 1.2; }
  .product .price { font-size: 30px; font-weight: 700; margin: 14px 0 0; color: var(--naranja); }
  .product ul, .item ul { margin: 8px 0 0; padding-left: 20px; }
  .kv { color: var(--gris); font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em; margin: 14px 0 2px; }
  .meter { height: 8px; background: var(--linea); border-radius: 99px; overflow: hidden; margin-top: 8px; }
  .meter span { display: block; height: 100%; background: var(--dolor); }
  .pieza .bloque .item-head { align-items: center; }
  .texto-copy { white-space: pre-wrap; word-break: break-word; margin: 10px 0 0; font: 15px/1.6 inherit;
    background: #f7f8fa; border-radius: 10px; padding: 14px; }
  .nota { color: var(--gris); font-size: 13px; }
  .ajuste { color: var(--objecion); font-size: 14px; background: #fff8e8; border-radius: 10px; padding: 10px 14px; }
  .copiar-bloque { padding: 6px 12px; font-size: 13px; }
  .sin-creditos { background: var(--papel); border: 2px solid var(--naranja); border-radius: 16px;
    padding: 24px; margin-top: 8px; }
  .sin-creditos h2 { margin: 0 0 10px; font-size: 20px; }
  .sin-creditos p { margin: 0 0 12px; }
  .fbbox { margin-top: 36px; background: var(--papel); border: 1px dashed var(--linea); border-radius: 16px; padding: 22px; }
  .fbbox textarea { min-height: 90px; margin-top: 12px; }
  .fbrow { display: flex; gap: 8px; margin-top: 12px; }
  .check { display: flex; gap: 10px; align-items: center; font-size: 15px; }
  .check input { width: auto; }
  .fbrow .tab[aria-pressed="true"] { background: var(--naranja); border-color: var(--naranja); color: #fff; }
  .extraidos .acciones { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
  .extraidos .tab { cursor: pointer; }
  .extraidos .tab.destacado { background: var(--naranja); border-color: var(--naranja); color: #fff; }
  .acciones-informe { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-top: 8px; }
  .acciones-informe .tab { cursor: pointer; }
  .recuperado { color: var(--gris); font-size: 14px; display: flex; align-items: center; gap: 10px;
    flex-wrap: wrap; margin-top: 18px; }
  .recuperado .tab { cursor: pointer; }
  .crudos { background: var(--papel); border: 1px solid var(--linea); border-radius: 14px; padding: 16px;
    max-height: 320px; overflow: auto; white-space: pre-wrap; word-break: break-word;
    font: 14px/1.6 inherit; color: #33445c; margin: 0; }
  .sources a { display: block; background: var(--papel); border: 1px solid var(--linea); border-radius: 14px;
    padding: 14px 18px; margin-bottom: 10px; text-decoration: none; }
  .sources small { color: var(--gris); display: block; }
"""

FONTS = """<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">"""

PIE_TEXTO = f"© {ANIO} Richard Taleno · Todos los derechos reservados · {CONTACT_EMAIL}"

SHELL_JS = """
(function () {
  const cuerpo = document.body;
  const btn = document.getElementById("hamb");
  const velo = document.getElementById("velo");
  const ancho = () => window.matchMedia("(max-width: 860px)").matches;
  try { if (localStorage.getItem("lateral") === "mini" && !ancho()) cuerpo.classList.add("mini"); } catch (e) {}
  if (btn) btn.addEventListener("click", () => {
    if (ancho()) {
      cuerpo.classList.toggle("abierta");
    } else {
      cuerpo.classList.toggle("mini");
      try { localStorage.setItem("lateral", cuerpo.classList.contains("mini") ? "mini" : "ancha"); } catch (e) {}
    }
  });
  if (velo) velo.addEventListener("click", () => cuerpo.classList.remove("abierta"));
  try {
    const ultima = cuerpo.dataset.ultimaNovedad || "";
    const vista = localStorage.getItem("novedades_vista") || "";
    const nav = document.getElementById("navNovedades");
    if (nav && ultima && ultima !== vista && !window.location.pathname.startsWith("/novedades")) {
      nav.insertAdjacentHTML("beforeend", '<span class="punto" title="Hay novedades"></span>');
    }
  } catch (e) {}
})();
"""


def _nombre_visible(user: str) -> str:
    if is_guest(user):
        email = guest_email(user)
        return (_read_leads().get(email, {}).get("nombre") or email.split("@")[0]).strip()
    return user.split("@")[0]


def _chip_creditos(user: str) -> str:
    """Etiqueta de saldo; vacía para las cuentas sin límite."""
    if not is_guest(user):
        return ""
    saldo = creditos_de(guest_email(user))
    if saldo < 0:
        return '<span class="chip">Acceso completo</span>'
    if saldo == 0:
        return (f'<a class="chip compra" href="{VENTA_URL}" target="_blank" rel="noopener">Sin créditos · Recargar</a>'
                if VENTA_URL else '<span class="chip">Sin créditos</span>')
    return f'<span class="chip">{saldo} crédito{"s" if saldo != 1 else ""}</span>'


def topbar(user: Optional[str], con_lateral: bool, mostrar_entrar: bool = True) -> str:
    hamb = '<button class="hamb" id="hamb" aria-label="Mostrar u ocultar el menú">☰</button>' if con_lateral else ""
    if user:
        derecha = f'<div class="usuario">{_chip_creditos(user)}</div>'
    elif mostrar_entrar:
        derecha = '<a class="btn-sesion primario" href="/login">Entrar</a>'
    else:
        derecha = ""
    return f"""<header class="topbar">{hamb}
      <a class="marca" href="/">TΛLENO <b>OS</b></a>
      <span class="sp"></span>{derecha}</header>"""


def sidebar(user: str, activo: str = "") -> str:
    nombre = _nombre_visible(user)
    inicial = (nombre[:1] or "?").upper()
    correo = guest_email(user) if is_guest(user) else user
    items = [f'<div class="ficha"><span class="avatar">{inicial}</span>'
             f'<span class="datos"><span class="nombre-usuario">{nombre}</span>'
             f'<span class="correo">{correo}</span></span></div>']
    items.append('<a class="nav" href="/" %s><span class="ic">▦</span><span class="tx">Panel</span></a>'
                 % ('aria-current="page"' if activo == "panel" else ""))
    items.append('<div class="grupo">Agentes</div>')
    iconos = {"radar": "◎", "analista": "⚑", "copy": "✎"}
    for slug, c in CEREBROS.items():
        actual = 'aria-current="page"' if activo == slug else ""
        items.append(f'<a class="nav" href="/cerebro/{slug}" {actual}>'
                     f'<span class="ic">{iconos.get(slug, "✦")}</span>'
                     f'<span class="tx">{c["nombre"]}</span></a>')
    items.append('<div class="nav mudo"><span class="ic">+</span><span class="tx">Próximamente</span></div>')
    items.append('<div class="grupo">Tu trabajo</div>')
    actual = 'aria-current="page"' if activo == "novedades" else ""
    items.append(f'<a class="nav" id="navNovedades" href="/novedades" {actual}>'
                 f'<span class="ic">★</span><span class="tx">Novedades</span></a>')
    actual = 'aria-current="page"' if activo == "historial" else ""
    items.append(f'<a class="nav" href="/historial" {actual}><span class="ic">◷</span>'
                 f'<span class="tx">Historial</span></a>')
    if not is_guest(user):
        items.append('<div class="grupo">Gestión</div>')
        actual = 'aria-current="page"' if activo == "leads" else ""
        items.append(f'<a class="nav" href="/leads" {actual}><span class="ic">✉</span>'
                     f'<span class="tx">Usuarios</span></a>')
        actual = 'aria-current="page"' if activo == "actividad" else ""
        items.append(f'<a class="nav" href="/actividad" {actual}><span class="ic">▲</span>'
                     f'<span class="tx">Actividad</span></a>')
        actual = 'aria-current="page"' if activo == "nichos" else ""
        items.append(f'<a class="nav" href="/nichos" {actual}><span class="ic">✦</span>'
                     f'<span class="tx">Nichos</span></a>')
    tele = (f'<a class="tele" href="{TELEGRAM_URL}" target="_blank" rel="noopener">✈ Canal de Telegram</a>'
            if TELEGRAM_URL else "")
    items.append('<div class="empuje"></div>')
    items.append('<a class="nav salir-nav" href="/logout">'
                 '<span class="ic">⏻</span><span class="tx">Salir</span></a>')
    items.append(f'<div class="pie-lateral">{tele}'
                 f'<span>© {ANIO} Richard Taleno</span>'
                 f'<span>Todos los derechos reservados</span>'
                 f'<a href="mailto:{CONTACT_EMAIL}">{CONTACT_EMAIL}</a></div>')
    return '<aside class="lateral">' + "".join(items) + '</aside><div class="velo" id="velo"></div>'


def page(title: str, contenido: str, user: Optional[str] = None, activo: str = "",
         extra_css: str = "", script: str = "", con_lateral: bool = True,
         mostrar_entrar: bool = True) -> str:
    lateral = sidebar(user, activo) if (con_lateral and user) else ""
    clase = "contenido" if lateral else "contenido solo"
    ultima = ultima_novedad() if lateral else ""
    return f"""<!DOCTYPE html>
<html lang="es"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>{title}</title>
{FONTS}
<style>{BASE_CSS}{extra_css}</style>
</head><body data-ultima-novedad="{ultima}">
{topbar(user, bool(lateral), mostrar_entrar)}
{lateral}
<main class="{clase}">{contenido}</main>
<script>{SHELL_JS}{script}</script>
</body></html>"""

RENDER_JS = """
const el = (id) => document.getElementById(id);

function esc(t) { const d = document.createElement("div"); d.textContent = t ?? ""; return d.innerHTML; }
const quotes = (arr, c) => (arr || []).map(q => `<blockquote style="--c:${c}">“${esc(q)}”</blockquote>`).join("");
const lista = (arr) => (arr && arr.length) ? `<ul>${arr.map(x => `<li>${esc(x)}</li>`).join("")}</ul>` : `<p class="empty">Sin evidencia en los comentarios.</p>`;

function renderCopy(data) {
  const c = data.copywriting;
  let html = acciones() + `<div class="summary"><p><strong>Prueba sugerida:</strong> ${esc(c.prueba_sugerida)}</p>
    <div class="meta">Basado en: ${esc(data.consulta || "")}</div></div>`;
  for (const pieza of (c.piezas || [])) {
    html += `<section class="pieza"><h2>${esc(pieza.formato)} <span class="count">· ${esc(pieza.angulo)}</span></h2>`;
    for (const b of (pieza.bloques || [])) {
      html += `<div class="item bloque">
        <div class="item-head"><h3>${esc(b.etiqueta)}</h3>
          <button type="button" class="tab copiar-bloque no-print">⧉ Copiar</button></div>
        <pre class="texto-copy">${esc(b.texto)}</pre>
        ${b.nota ? `<p class="nota">${esc(b.nota)}</p>` : ""}</div>`;
    }
    if (pieza.frases_usadas && pieza.frases_usadas.length) {
      html += `<div class="item"><h3>Frases del mercado usadas</h3><ul>` +
        pieza.frases_usadas.map(f => `<li>“${esc(f.frase)}” → ${esc(f.uso)}</li>`).join("") + `</ul></div>`;
    }
    if (pieza.ajuste_meta) html += `<p class="ajuste">Ajuste Meta: ${esc(pieza.ajuste_meta)}</p>`;
    html += `</section>`;
  }
  return html;
}

function acciones() {
  return `<div class="acciones-informe no-print">
    <button type="button" class="tab" id="pdf">⤓ Guardar PDF</button>
    <button type="button" class="tab" id="copiarInforme">⧉ Copiar informe</button>
    <span id="accionStatus" class="counter"></span>
  </div>`;
}

function informeTexto(data) {
  const L = [];
  const linea = (t) => L.push(t);
  const NL = "\\n";
  if (data.mercado) {
    const m = data.mercado, v = m.veredicto, p = m.propuesta_producto, pu = m.problema_urgente;
    linea(`ANALISTA DE MERCADO — ${data.consulta || ""}`);
    linea(`${data.total_comentarios} comentarios · ${data.fuente}` + NL);
    linea("RESUMEN" + NL + m.resumen_ejecutivo + NL);
    linea(`VEREDICTO: ${v.recomendacion} (${v.puntuacion}/10)` + NL + v.justificacion);
    if (v.riesgos && v.riesgos.length) linea(`Riesgos: ${v.riesgos.join(" · ")}`);
    linea(`Siguiente paso: ${v.siguiente_paso}` + NL);
    linea(`PROBLEMA MACRO (evitar): ${pu.problema_macro_a_evitar}`);
    linea(`PROBLEMA A ATACAR: "${pu.problema_urgente_especifico}"` + NL + pu.por_que_es_especifico + NL);
    linea(`PRODUCTO: ${p.nombre}`);
    linea(`Mecanismo: ${p.mecanismo_unico}`);
    linea(`Promesa: ${p.promesa}`);
    linea(`Formato: ${p.formato}`);
    if (p.que_incluye && p.que_incluye.length) linea(`Incluye: ${p.que_incluye.join(", ")}`);
    linea(`Precio sugerido: US$ ${p.precio_sugerido_usd} — ${p.justificacion_precio}` + NL);
    linea("FRASES QUE SE REPITEN");
    (m.frases_repetidas || []).forEach(f => linea(`- "${f.frase}" (${f.frecuencia}): ${f.que_revela}`));
    linea(NL + "LO QUE YA INTENTARON Y NO FUNCIONÓ");
    (m.intentos_fallidos || []).forEach(i => linea(`- ${i.que_intentaron}: ${i.por_que_fallo}`));
    linea(NL + "RESULTADO QUE REALMENTE BUSCAN");
    linea(`Dicen: ${m.resultado_deseado.lo_que_dicen}`);
    linea(`En realidad: ${m.resultado_deseado.lo_que_realmente_buscan}`);
    linea(NL + `DOLOR EMOCIONAL: ${m.dolor_emocional.intensidad}/10 (${(m.dolor_emocional.emociones || []).join(", ")})`);
    linea(m.dolor_emocional.costo_de_seguir_igual);
    linea(NL + `DISPOSICIÓN A PAGAR: ${m.disposicion_a_pagar.nivel}`);
    (m.disposicion_a_pagar.senales || []).forEach(x => linea(`- ${x}`));
    linea(`En qué ya gastan: ${m.disposicion_a_pagar.en_que_ya_gastan}`);
    linea(NL + "BRECHA DE OPORTUNIDAD");
    linea(m.brecha_oportunidad.descripcion);
    linea(m.brecha_oportunidad.por_que_no_esta_resuelto);
  } else if (data.copywriting) {
    linea(`COPY — ${data.consulta || ""}` + NL);
    (data.copywriting.piezas || []).forEach(p => {
      linea(`--- ${p.formato} · ${p.angulo} ---`);
      (p.bloques || []).forEach(b => {
        linea(`[${b.etiqueta}]${b.nota ? " (" + b.nota + ")" : ""}`);
        linea(b.texto + NL);
      });
      if (p.frases_usadas && p.frases_usadas.length) {
        linea("Frases del mercado usadas:");
        p.frases_usadas.forEach(f => linea(`- "${f.frase}" → ${f.uso}`));
      }
      if (p.ajuste_meta) linea(`Ajuste Meta: ${p.ajuste_meta}`);
      linea("");
    });
    linea(`Prueba sugerida: ${data.copywriting.prueba_sugerida}`);
  } else if (data.analisis) {
    const a = data.analisis;
    linea(`RADAR — ${data.consulta || ""}`);
    linea(`${data.total_comentarios} comentarios · ${data.fuente}` + NL);
    linea("RESUMEN" + NL + a.resumen + NL);
    [["dolores", "DOLORES"], ["objeciones", "OBJECIONES"], ["deseos", "DESEOS"]].forEach(([k, t]) => {
      linea(t);
      (a[k] || []).forEach(i => {
        linea(`- ${i.tema} (frecuencia ${i.frecuencia})`);
        (i.ejemplos || []).forEach(q => linea(`    "${q}"`));
      });
      linea("");
    });
  }
  linea(NL + "Generado con TΛLENO OS · __CONTACTO__");
  return L.join(NL);
}

function fuentes(data) {
  if (!data.videos || !data.videos.length) return "";
  return `<section class="sources"><h2>Videos analizados</h2>` + data.videos.map(v =>
    `<a href="${esc(v.url)}" target="_blank" rel="noopener">${esc(v.titulo)}<small>${esc(v.canal)} · ${v.comentarios_analizados} comentarios</small></a>`).join("") + `</section>`;
}

function renderRadar(data) {
  const a = data.analisis;
  const CATS = [["dolores", "Dolores", "var(--dolor)"], ["objeciones", "Objeciones", "var(--objecion)"], ["deseos", "Deseos", "var(--deseo)"]];
  let html = acciones() + `<div class="summary"><p>${esc(a.resumen)}</p><div class="meta">${data.total_comentarios} comentarios · ${esc(data.fuente)}</div></div>`;
  for (const [key, label, color] of CATS) {
    const items = a[key] || [];
    html += `<section style="--c:${color}"><h2><span class="dot"></span>${label} <span class="count">(${items.length})</span></h2>`;
    if (!items.length) html += `<p class="empty">No aparecen en estos comentarios.</p>`;
    for (const it of items) html += `<div class="item"><div class="item-head"><h3>${esc(it.tema)}</h3><span class="freq">Frecuencia ${esc(it.frecuencia)}</span></div>${quotes(it.ejemplos, color)}</div>`;
    html += `</section>`;
  }
  return html + bloqueComentarios(data) + fuentes(data);
}

function colorVeredicto(rec) {
  const r = (rec || "").toUpperCase();
  if (r.startsWith("NO")) return "var(--no)";
  if (r.startsWith("VALID")) return "var(--tal)";
  return "var(--si)";
}

function botonCopy(data) {
  if (!data.id_analisis) return "";
  return `<div class="acciones-informe no-print">
    <a class="tab destacado" href="/cerebro/copy?analisis=${encodeURIComponent(data.id_analisis)}">✎ Escribir el contenido</a>
  </div>`;
}

function renderAnalista(data) {
  const m = data.mercado, v = m.veredicto, p = m.propuesta_producto, pu = m.problema_urgente;
  const d = m.dolor_emocional, pay = m.disposicion_a_pagar, r = m.resultado_deseado, b = m.brecha_oportunidad;
  let html = acciones() + `<div class="summary"><p>${esc(m.resumen_ejecutivo)}</p><div class="meta">${data.total_comentarios} comentarios · ${esc(data.fuente)}</div></div>`;
  html += `<div class="verdict" style="--v:${colorVeredicto(v.recomendacion)}">
    <div class="top"><span class="rec">${esc(v.recomendacion)}</span><span class="score">Oportunidad ${v.puntuacion}/10</span></div>
    <p>${esc(v.justificacion)}</p>
    ${v.riesgos && v.riesgos.length ? `<p><strong>Riesgos:</strong> ${v.riesgos.map(esc).join(" · ")}</p>` : ""}
    <p class="next"><strong>Siguiente paso (7 días):</strong> ${esc(v.siguiente_paso)}</p></div>`;
  html += `<section><h2>El problema urgente específico</h2><div class="item problem">
    <div class="kv">Problema macro (evitar)</div><div class="macro">${esc(pu.problema_macro_a_evitar)}</div>
    <div class="kv">Problema a atacar</div><p class="specific">“${esc(pu.problema_urgente_especifico)}”</p>
    <p>${esc(pu.por_que_es_especifico)}</p></div></section>`;
  html += `<section><h2><span class="step">7</span> Propuesta de producto</h2><div class="item product">
    <p class="nombre">${esc(p.nombre)}</p>
    <div class="kv">Mecanismo único</div><div>${esc(p.mecanismo_unico)}</div>
    <div class="kv">Promesa</div><div>${esc(p.promesa)}</div>
    <div class="kv">Formato</div><div>${esc(p.formato)}</div>
    <div class="kv">Incluye</div>${lista(p.que_incluye)}
    <p class="price">US$ ${Number(p.precio_sugerido_usd).toFixed(p.precio_sugerido_usd % 1 ? 2 : 0)}</p>
    <div>${esc(p.justificacion_precio)}</div></div></section>`;
  html += `<section><h2><span class="step">1</span> Frases que se repiten</h2>`;
  html += (m.frases_repetidas || []).length ? m.frases_repetidas.map(f => `<div class="item"><div class="item-head"><h3>“${esc(f.frase)}”</h3><span class="freq">Frecuencia ${esc(f.frecuencia)}</span></div><p>${esc(f.que_revela)}</p></div>`).join("") : `<p class="empty">Sin frases repetidas claras.</p>`;
  html += `</section>`;
  html += `<section><h2><span class="step">2</span> Lo que ya intentaron y no funcionó</h2>`;
  html += (m.intentos_fallidos || []).length ? m.intentos_fallidos.map(i => `<div class="item"><h3>${esc(i.que_intentaron)}</h3><p>${esc(i.por_que_fallo)}</p>${quotes([i.ejemplo], "var(--objecion)")}</div>`).join("") : `<p class="empty">No mencionan intentos previos.</p>`;
  html += `</section>`;
  html += `<section><h2><span class="step">3</span> El resultado que realmente buscan</h2><div class="item">
    <div class="kv">Lo que dicen</div><div>${esc(r.lo_que_dicen)}</div>
    <div class="kv">Lo que realmente buscan</div><div><strong>${esc(r.lo_que_realmente_buscan)}</strong></div>${quotes(r.evidencia, "var(--deseo)")}</div></section>`;
  html += `<section><h2><span class="step">4</span> Dolor emocional</h2><div class="item">
    <div class="item-head"><h3>Intensidad ${d.intensidad}/10</h3><span class="freq" style="--c:var(--dolor)">${(d.emociones || []).map(esc).join(" · ")}</span></div>
    <div class="meter"><span style="width:${d.intensidad * 10}%"></span></div>
    <div class="kv">Si en un mes siguen igual</div><div>${esc(d.costo_de_seguir_igual)}</div>${quotes(d.evidencia, "var(--dolor)")}</div></section>`;
  html += `<section><h2><span class="step">5</span> Disposición a pagar</h2><div class="item">
    <h3>Nivel ${esc(pay.nivel)}</h3>${lista(pay.senales)}
    <div class="kv">En qué ya gastan</div><div>${esc(pay.en_que_ya_gastan)}</div></div></section>`;
  html += `<section><h2><span class="step">6</span> Brecha de oportunidad</h2><div class="item">
    <h3>${esc(b.descripcion)}</h3><p>${esc(b.por_que_no_esta_resuelto)}</p>${quotes(b.evidencia, "var(--azul)")}</div></section>`;
  return html + fuentes(data);
}


let comentariosExtraidos = [];

function bloqueComentarios(data) {
  if (!data.comentarios || !data.comentarios.length) return "";
  comentariosExtraidos = data.comentarios;
  return `<section class="extraidos">
    <h2>Comentarios extraídos <span class="count">(${data.comentarios.length})</span></h2>
    <div class="acciones">
      <button type="button" class="tab" id="copiar">⧉ Copiar todos</button>
      <button type="button" class="tab destacado" id="alAnalista">→ Analizar con el Analista</button>
    </div>
    <p id="copiaStatus" class="counter"></p>
    <pre class="crudos">${esc(data.comentarios.join("\\n"))}</pre>
  </section>`;
}

let ultimoInforme = null;

async function alPortapapeles(texto, mensaje) {
  try {
    await navigator.clipboard.writeText(texto);
  } catch (err) {
    const ta = document.createElement("textarea");
    ta.value = texto; document.body.appendChild(ta); ta.select();
    document.execCommand("copy"); ta.remove();
  }
  const s = el("accionStatus") || el("copiaStatus");
  if (s) s.textContent = mensaje;
}

function conectarBotones(data) {
  if (data) ultimoInforme = data;
  const pdf = el("pdf"), copiarInf = el("copiarInforme");
  if (pdf) pdf.addEventListener("click", () => window.print());
  if (copiarInf) copiarInf.addEventListener("click", () =>
    alPortapapeles(informeTexto(ultimoInforme || {}), "Informe copiado como texto."));
  document.querySelectorAll(".copiar-bloque").forEach(b => b.addEventListener("click", () => {
    const pre = b.closest(".bloque").querySelector(".texto-copy");
    alPortapapeles(pre.textContent, "Bloque copiado.");
    b.textContent = "✓ Copiado";
    setTimeout(() => { b.textContent = "⧉ Copiar"; }, 1800);
  }));
  const copiar = el("copiar"), alAnalista = el("alAnalista");
  if (copiar) copiar.addEventListener("click", () =>
    alPortapapeles(comentariosExtraidos.join("\\n"),
      `${comentariosExtraidos.length} comentarios copiados al portapapeles.`));
  if (alAnalista) alAnalista.addEventListener("click", () => {
    try { sessionStorage.setItem("comentarios_radar", comentariosExtraidos.join("\\n")); } catch (e) {}
    window.location.href = "/cerebro/analista";
  });
}


function pintarSegunAgente(data) {
  if (data.copywriting) return renderCopy(data);
  if (data.mercado) return renderAnalista(data);
  return renderRadar(data);
}

"""


BRAIN_JS = """
const MODO = "__MODO__";
const SOURCES = {
  "paste": { endpoint: "/paste", hint: "Pega los comentarios, uno por línea. Lo ideal son 200-300.",
    wait: "Analizando los comentarios.", body: () => ({ texto: el("texto").value }) },
  "yt-search": { endpoint: "/youtube/search", type: "text", placeholder: "Ej: cómo emprender con poco dinero",
    hint: "Escribe un tema. Buscamos videos en español con más comentarios y los analizamos juntos.",
    wait: "Buscando videos y leyendo sus comentarios.", body: () => ({ query: el("field").value.trim(), max_videos: Number(el("nvideos").value) }) },
  "yt-video": { endpoint: "/youtube/video", type: "url", placeholder: "https://www.youtube.com/watch?v=...",
    hint: "Pega el enlace de un video de YouTube (también sirven Shorts).",
    wait: "Leyendo los comentarios del video.", body: () => ({ url: el("field").value.trim() }) },
  "facebook": { endpoint: "/analyze", type: "url", placeholder: "https://www.facebook.com/...",
    hint: "Pega el enlace de una publicación pública de Facebook. Puede tardar de 1 a 3 minutos.",
    wait: "Leyendo comentarios de Facebook.", body: () => ({ url: el("field").value.trim() }) },
};
const BLOQUEADAS = __BLOQUEADAS__;
let src = "__INICIAL__";

function avisoSuscriptor() {
  el("status").className = "status aviso";
  el("status").innerHTML = "🔒 <strong>Facebook es para suscriptores.</strong> " +
    "Extraer comentarios de Facebook tiene un costo por publicación, así que está reservado al plan de pago. " +
    "Mientras tanto, YouTube y pegar comentarios funcionan sin límite de fuentes.";
}

function setSource(s) {
  if (BLOQUEADAS.includes(s)) { avisoSuscriptor(); return; }
  src = s; const cfg = SOURCES[s];
  document.querySelectorAll("#sources .tab").forEach(t => t.setAttribute("aria-pressed", String(t.dataset.src === s)));
  el("pasteBox").hidden = s !== "paste";
  el("lineBox").hidden = s === "paste";
  el("texto").required = s === "paste";
  el("field").required = s !== "paste";
  if (cfg.type) { el("field").type = cfg.type; el("field").placeholder = cfg.placeholder; el("field").value = ""; }
  el("nvideos").hidden = s !== "yt-search";
  el("hint").textContent = cfg.hint;
}
document.querySelectorAll("#sources .tab").forEach(t => t.addEventListener("click", () => setSource(t.dataset.src)));
el("texto").addEventListener("input", () => {
  const n = el("texto").value.split("\\n").filter(l => l.trim().length >= 3).length;
  el("counter").textContent = `${n} comentarios detectados` + (n && n < 50 ? " · con menos de 50 el análisis será poco confiable" : "");
});
setSource(src);

// Si venimos del Radar, precargamos los comentarios que trajo
try {
  const traidos = sessionStorage.getItem("comentarios_radar");
  if (traidos && el("texto")) {
    sessionStorage.removeItem("comentarios_radar");
    el("texto").value = traidos;
    el("texto").dispatchEvent(new Event("input"));
    el("status").textContent = "Comentarios traídos del Radar. Dale a Analizar.";
    el("texto").scrollIntoView({ behavior: "smooth", block: "center" });
  }
} catch (e) {}

const CLAVE_ULTIMO = "ultimo_resultado_" + MODO;
function guardarResultado(data) {
  try { sessionStorage.setItem(CLAVE_ULTIMO, JSON.stringify({ data: data, fecha: Date.now() })); } catch (e) {}
}

function pintar(data, recuperado) {
  el("results").innerHTML = pintarSegunAgente(data) +
    (recuperado ? `<p class="recuperado">Este es tu último análisis, guardado en este navegador.
      <button type="button" class="tab" id="limpiar">Borrar y empezar de nuevo</button></p>` : "");
  conectarBotones(data);
  const limpiar = el("limpiar");
  if (limpiar) limpiar.addEventListener("click", () => {
    try { sessionStorage.removeItem(CLAVE_ULTIMO); } catch (e) {}
    el("results").innerHTML = ""; el("fbBox").hidden = true; el("status").textContent = "";
  });
  el("fbBox").hidden = false;
}

function recuperarResultado() {
  try {
    const guardado = sessionStorage.getItem(CLAVE_ULTIMO);
    if (!guardado) return;
    const { data } = JSON.parse(guardado);
    if (data) pintar(data, true);
  } catch (e) {}
}

let fbUtil = null;
document.querySelectorAll(".fbrow .tab").forEach(b => b.addEventListener("click", () => {
  fbUtil = b.dataset.util === "1";
  document.querySelectorAll(".fbrow .tab").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
}));
el("fbEnviar").addEventListener("click", async () => {
  const s = el("fbStatus");
  try {
    await fetch("/api/feedback", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ util: fbUtil, texto: el("fbTexto").value }) });
    s.className = "status"; s.textContent = "¡Gracias! Lo leo todo.";
    el("fbTexto").value = "";
  } catch (err) { s.className = "status error"; s.textContent = "No se pudo enviar."; }
});

recuperarResultado();

let analizando = false;
window.addEventListener("beforeunload", (e) => {
  if (analizando) { e.preventDefault(); e.returnValue = ""; }
});

el("form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const cfg = SOURCES[src];
  el("status").className = "status";
  el("status").innerHTML = `<span class="spinner"></span>${cfg.wait}`;
  el("btn").disabled = true; el("btn").textContent = "Analizando…"; analizando = true;
  try {
    const body = { ...cfg.body(), modo: MODO, nicho: el("nicho").value.trim() || null };
    const res = await fetch(cfg.endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (res.status === 401) { window.location.href = "/login"; return; }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      let msg = "Revisa lo que escribiste e inténtalo de nuevo.";
      if (typeof data.detail === "string") msg = data.detail;
      else if (Array.isArray(data.detail) && data.detail[0]) msg = "Dato no válido: " + (data.detail[0].msg || "");
      throw new Error(msg);
    }
    el("status").textContent = "";
    guardarResultado(data);
    pintar(data, false);
    el("results").scrollIntoView({ behavior: "smooth", block: "start" });
    if (data.restantes !== null && data.restantes !== undefined) {
      el("status").className = "status";
      el("status").textContent = data.restantes > 0
        ? `Te quedan ${data.restantes} análisis de prueba.`
        : "Fue tu último análisis de prueba. Escríbeme si quieres acceso completo.";
    }
  } catch (err) {
    el("status").className = "status error";
    el("status").textContent = "No se pudo analizar: " + err.message;
  } finally {
    el("btn").disabled = false; el("btn").textContent = "Analizar"; analizando = false;
  }
});
"""



LOGIN_CSS = """
  .caja { max-width: 420px; margin: 6vh auto; background: var(--papel); border: 1px solid var(--linea);
    border-radius: 18px; padding: 32px 26px; }
  .caja h1 { font-size: 26px; margin-bottom: 6px; }
  .caja > p { color: var(--gris); margin: 0 0 22px; }
  .campo { margin-bottom: 12px; }
  .invitado { margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--linea); }
  .invitado > span { display: block; font-weight: 700; margin-bottom: 12px; }
  .invitado .submit { background: var(--tinta); }
  .letra-chica { color: var(--gris); font-size: 12px; line-height: 1.5; margin: 14px 0 0; }
  .olvide { margin: 12px 0 0; text-align: center; }
  .olvide a { color: var(--gris); font-size: 13px; text-decoration: none; }
  .olvide a:hover { color: var(--naranja); }
  #cajaOlvide { margin-top: 12px; }
  #cajaOlvide .submit { background: var(--tinta); }
  .acceso { margin: 20px 0 0; padding-top: 16px; border-top: 1px solid var(--linea); text-align: center; }
  .acceso a { color: var(--gris); font-size: 14px; text-decoration: none; }
  .acceso a:hover { color: var(--naranja); }
  #cajaAdmin { margin-top: 14px; }
  #cajaAdmin .submit { background: var(--tinta); }
"""


LOGIN_JS = """
const verRegistro = document.getElementById("verRegistro");
if (verRegistro) verRegistro.addEventListener("click", (e) => {
  e.preventDefault();
  const caja = document.getElementById("cajaRegistro");
  caja.hidden = !caja.hidden;
  verRegistro.textContent = caja.hidden ? "¿Primera vez? Crear cuenta gratis" : "Ya tengo cuenta";
  if (!caja.hidden) document.getElementById("rnombre").focus();
});

async function enviar(url, cuerpo, estado) {
  const s = document.getElementById(estado);
  s.className = "status"; s.innerHTML = '<span class="spinner"></span>Un momento…';
  try {
    const res = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cuerpo) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail
      : (Array.isArray(data.detail) && data.detail[0] ? data.detail[0].msg : "No se pudo entrar."));
    window.location.href = "/";
  } catch (err) { s.className = "status error"; s.textContent = err.message; }
}

const verOlvide = document.getElementById("verOlvide");
if (verOlvide) verOlvide.addEventListener("click", (e) => {
  e.preventDefault();
  const caja = document.getElementById("cajaOlvide");
  caja.hidden = !caja.hidden;
  verOlvide.textContent = caja.hidden ? "¿Olvidaste tu contraseña?" : "Volver a entrar";
  if (!caja.hidden) document.getElementById("oemail").focus();
});

const formOlvide = document.getElementById("olvide");
if (formOlvide) formOlvide.addEventListener("submit", async (e) => {
  e.preventDefault();
  const s = document.getElementById("ostatus");
  s.className = "status"; s.innerHTML = '<span class="spinner"></span>Enviando…';
  try {
    const res = await fetch("/api/recuperar", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: document.getElementById("oemail").value.trim() }) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : "No se pudo enviar.");
    formOlvide.hidden = true;
    s.className = "status aviso";
    s.innerHTML = "📬 <strong>Si ese correo tiene cuenta, te llegó un enlace.</strong> " +
      "Vale 30 minutos. Si no lo ves, revisa spam.";
  } catch (err) { s.className = "status error"; s.textContent = err.message; }
});

const form = document.getElementById("login");
if (form) form.addEventListener("submit", (e) => {
  e.preventDefault();
  enviar("/api/login", { email: document.getElementById("email").value.trim(),
                         password: document.getElementById("password").value }, "status");
});

const formReg = document.getElementById("registro");
if (formReg) formReg.addEventListener("submit", (e) => {
  e.preventDefault();
  const pass = document.getElementById("rpass").value;
  if (pass.length < 8) {
    const s = document.getElementById("rstatus");
    s.className = "status error"; s.textContent = "La contraseña necesita al menos 8 caracteres.";
    return;
  }
  enviar("/api/registro", { nombre: document.getElementById("rnombre").value.trim(),
                            email: document.getElementById("remail").value.trim(),
                            password: pass }, "rstatus");
});
"""


def login_page(msg: str = "") -> str:
    aviso = f'<p class="status error">{msg}</p>' if msg else ""
    registro = (f"""<p class="acceso"><a href="#" id="verRegistro">¿Primera vez? Crear cuenta gratis</a></p>
      <div id="cajaRegistro" hidden>
        <form id="registro">
          <div class="campo"><input id="rnombre" required placeholder="Tu nombre" aria-label="Nombre"></div>
          <div class="campo"><input id="remail" type="email" required placeholder="Tu correo" aria-label="Correo"></div>
          <div class="campo"><input id="rpass" type="password" required minlength="8"
            placeholder="Crea una contraseña (mínimo 8)" aria-label="Contraseña"></div>
          <button class="submit" type="submit">Crear mi cuenta</button>
        </form>
        <p id="rstatus" class="status"></p>
        <p class="letra-chica">Te damos {GUEST_LIMIT} créditos para probar. El Radar es gratis siempre.<br>
          Tus análisis se guardan en tu cuenta. Revisamos el uso para mejorar los agentes. Sin spam.</p>
      </div>""" if GUEST_MODE else "")

    contenido = f"""<div class="caja">
      <h1>Entrar</h1>
      <p>Tus agentes de investigación de mercado, en un solo lugar.</p>
      {aviso}
      <form id="login">
        <div class="campo"><input id="email" type="email" required placeholder="Tu correo" aria-label="Correo"></div>
        <div class="campo"><input id="password" type="password" required placeholder="Tu contraseña" aria-label="Contraseña"></div>
        <button class="submit" type="submit">Entrar</button>
      </form>
      <p id="status" class="status"></p>
      <p class="olvide"><a href="#" id="verOlvide">¿Olvidaste tu contraseña?</a></p>
      <div id="cajaOlvide" hidden>
        <form id="olvide">
          <div class="campo"><input id="oemail" type="email" required placeholder="Tu correo" aria-label="Correo"></div>
          <button class="submit" type="submit">Enviarme el enlace</button>
        </form>
        <p id="ostatus" class="status"></p>
      </div>
      {registro}
    </div>"""
    return page("Entrar · TΛLENO OS", contenido, None, "", LOGIN_CSS, LOGIN_JS,
                con_lateral=False, mostrar_entrar=False)


NUEVA_CLAVE_JS = """
document.getElementById("formNueva").addEventListener("submit", async (e) => {
  e.preventDefault();
  const s = document.getElementById("nstatus");
  const p1 = document.getElementById("np1").value, p2 = document.getElementById("np2").value;
  if (p1.length < 8) { s.className = "status error"; s.textContent = "Mínimo 8 caracteres."; return; }
  if (p1 !== p2) { s.className = "status error"; s.textContent = "Las dos contraseñas no coinciden."; return; }
  s.className = "status"; s.innerHTML = '<span class="spinner"></span>Guardando…';
  try {
    const res = await fetch("/api/recuperar/confirmar", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: document.getElementById("token").value, password: p1 }) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : "No se pudo guardar.");
    window.location.href = "/";
  } catch (err) { s.className = "status error"; s.textContent = err.message; }
});
"""


def nueva_clave_page(token: str, email: str) -> str:
    contenido = f"""<div class="caja">
      <h1>Elige una contraseña nueva</h1>
      <p>Para la cuenta <strong>{email}</strong>.</p>
      <form id="formNueva">
        <input type="hidden" id="token" value="{token}">
        <div class="campo"><input id="np1" type="password" required minlength="8"
          placeholder="Contraseña nueva (mínimo 8)" aria-label="Contraseña nueva"></div>
        <div class="campo"><input id="np2" type="password" required minlength="8"
          placeholder="Repítela" aria-label="Repite la contraseña"></div>
        <button class="submit" type="submit">Guardar y entrar</button>
      </form>
      <p id="nstatus" class="status"></p>
    </div>"""
    return page("Nueva contraseña · TΛLENO OS", contenido, None, "", LOGIN_CSS, NUEVA_CLAVE_JS,
                con_lateral=False, mostrar_entrar=False)


DASH_CSS = """
  .cerebros { display: grid; gap: 14px; grid-template-columns: 1fr; }
  @media (min-width: 900px) { .cerebros { grid-template-columns: 1fr 1fr; } }
  .cerebro { background: var(--papel); border: 1px solid var(--linea); border-radius: 18px; padding: 22px;
    text-decoration: none; display: flex; flex-direction: column; gap: 8px; transition: border-color .15s, transform .15s; }
  .cerebro:hover { border-color: var(--naranja); transform: translateY(-2px); }
  .cerebro .num { font-size: 12px; font-weight: 700; letter-spacing: .08em; color: var(--naranja); }
  .cerebro .nombre { font-size: 21px; font-weight: 700; }
  .cerebro .lema { color: var(--azul); font-weight: 600; font-size: 14px; }
  .cerebro .desc { color: var(--gris); font-size: 15px; }
  .cerebro .pie { margin-top: auto; padding-top: 12px; font-size: 13px; color: var(--gris); }
  .proximo { border-style: dashed; }
  .proximo .nombre { color: var(--gris); }
"""


def dash_page(user: str) -> str:
    tarjetas = ""
    for i, (slug, c) in enumerate(CEREBROS.items(), start=1):
        tarjetas += f"""<a class="cerebro" href="/cerebro/{slug}">
          <span class="num">AGENTE {i:02d}</span>
          <span class="nombre">{c['nombre']}</span>
          <span class="lema">{c['lema']}</span>
          <span class="desc">{c['desc']}</span>
          <span class="pie">⏱ {c['tiempo']}</span></a>"""
    if not is_guest(user):
        tarjetas += """<a class="cerebro proximo" href="/leads">
          <span class="num">PANEL</span><span class="nombre">Usuarios</span>
          <span class="desc">Usuarios registrados, uso y feedback recibido.</span></a>"""
    tarjetas += """<div class="cerebro proximo">
          <span class="num">PRÓXIMAMENTE</span><span class="nombre">Nuevo agente</span>
          <span class="desc">Aquí van los siguientes: copy, oferta, contenido…</span></div>"""
    contenido = f"""<h1>Hola, {_nombre_visible(user)}</h1>
      <p class="intro">Cada agente hace una sola cosa, y la hace bien. Elige con cuál vas a trabajar hoy.</p>
      <div class="cerebros">{tarjetas}</div>"""
    return page("Panel · TΛLENO OS", contenido, user, "panel", DASH_CSS)


LEADS_CSS = """
  table { width: 100%; border-collapse: collapse; background: var(--papel); border-radius: 14px; overflow: hidden; }
  th, td { text-align: left; padding: 12px 14px; border-bottom: 1px solid var(--linea); font-size: 14px; vertical-align: top; }
  th { background: #fafbfc; font-size: 12px; text-transform: uppercase; letter-spacing: .06em; color: var(--gris); }
  .wrap { overflow-x: auto; }
  .fb { color: var(--gris); font-size: 13px; }
  .descarga { display: inline-block; margin-bottom: 16px; font-weight: 700; color: var(--naranja); text-decoration: none; }
  .cargar { display: flex; gap: 5px; flex-wrap: wrap; margin-top: 8px; }
  .mini { font: 600 12px inherit; padding: 5px 9px; border: 1px solid var(--linea); border-radius: 8px;
    background: var(--papel); cursor: pointer; color: var(--gris); }
  .mini:hover { border-color: var(--naranja); color: var(--naranja); }
  .mini.inf { border-style: dashed; }
  .mini.borrar-usuario:hover { border-color: var(--no); color: var(--no); }
  tr[hidden] { display: none; }
"""

LEADS_JS = """
const buscador = document.getElementById("buscar");
if (buscador) buscador.addEventListener("input", () => {
  const q = buscador.value.trim().toLowerCase();
  document.querySelectorAll("tr[data-buscar]").forEach(tr => {
    tr.hidden = q !== "" && !tr.dataset.buscar.includes(q);
  });
});

document.querySelectorAll(".mini").forEach(b => b.addEventListener("click", async () => {
  let cuerpo;
  if (b.dataset.ilimitado !== undefined) {
    cuerpo = { email: b.dataset.email, ilimitado: b.dataset.ilimitado === "1" };
  } else if (b.dataset.otra) {
    const n = prompt("¿Cuántos créditos? Usa un número negativo para quitar.", "10");
    if (n === null || Number.isNaN(Number(n)) || Number(n) === 0) return;
    cuerpo = { email: b.dataset.email, creditos: Number(n) };
  } else if (b.dataset.clave) {
    if (!confirm("¿Generar una contraseña nueva? La anterior dejará de servir.")) return;
    b.disabled = true;
    const r = await fetch("/api/usuarios/" + encodeURIComponent(b.dataset.email) + "/clave", { method: "POST" });
    const d = await r.json().catch(() => ({}));
    b.disabled = false;
    if (r.ok) prompt("Contraseña nueva. Cópiala y mándasela:", d.clave);
    else alert("No se pudo generar.");
    return;
  } else if (b.classList.contains("borrar-usuario")) {
    const nombre = b.dataset.nombre;
    if (!confirm("¿Eliminar a " + nombre + "? Se borran también sus análisis guardados. No se puede deshacer.")) return;
    if (prompt("Para confirmar, escribe BORRAR") !== "BORRAR") return;
    b.disabled = true;
    const r = await fetch("/api/usuarios/" + encodeURIComponent(b.dataset.email), { method: "DELETE" });
    if (r.ok) window.location.reload(); else { b.disabled = false; alert("No se pudo eliminar."); }
    return;
  } else {
    cuerpo = { email: b.dataset.email, creditos: Number(b.dataset.n) };
  }
  b.disabled = true;
  const res = await fetch("/api/creditos", { method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(cuerpo) });
  if (res.ok) window.location.reload(); else { b.disabled = false; alert("No se pudo actualizar."); }
}));
"""


def leads_page(user: str) -> str:
    leads = sorted(_read_leads().values(), key=lambda l: l.get("creado", ""), reverse=True)
    filas = ""
    for l in leads:
        fb = "<br>".join(
            f"{'👍' if f.get('util') else '👎'} {(f.get('texto') or '').replace('<', '&lt;')}"
            for f in l.get("feedback", [])
        ) or "<span class='fb'>Sin feedback</span>"
        sync = "✓ en systeme.io" if l.get("systeme") else ("· solo local" if SYSTEME_API_KEY else "")
        correo = l.get('email', '')
        saldo = ("∞" if l.get("ilimitado") else int(l.get("creditos", 0) or 0))
        botones = (f'<div class="cargar">'
                   f'<button class="mini" data-email="{correo}" data-n="15">+15 ($7)</button>'
                   f'<button class="mini" data-email="{correo}" data-n="45">+45 ($17)</button>'
                   f'<button class="mini" data-email="{correo}" data-otra="1">Otra…</button>'
                   f'<button class="mini inf" data-email="{correo}" data-ilimitado="{"0" if l.get("ilimitado") else "1"}">'
                   f'{"Quitar ∞" if l.get("ilimitado") else "∞"}</button>'
                   f'<button class="mini" data-email="{correo}" data-clave="1">Clave nueva</button>'
                   f'<button class="mini borrar-usuario" data-email="{correo}" '
                   f'data-nombre="{(l.get("nombre") or correo)}">Eliminar</button></div>')
        filas += f"""<tr data-buscar="{(l.get('nombre','') + ' ' + correo).lower()}"><td>{l.get('nombre','')}<br><span class="fb">{correo}</span><br>
          <span class="fb">{sync}</span></td>
          <td><strong>{saldo}</strong><br><span class="fb">{l.get('usos',0)} usos</span>{botones}</td>
          <td>{l.get('creado','')[:10]}</td><td>{fb}</td></tr>"""
    if not filas:
        filas = '<tr><td colspan="4">Todavía no hay usuarios registrados.</td></tr>'
    contenido = f"""<h1>Usuarios</h1>
      <p class="intro">{len(leads)} usuarios registrados. El feedback que dejan aparece en la última columna.</p>
      <div class="row" style="margin-bottom:14px">
        <input id="buscar" placeholder="Buscar por nombre o correo" aria-label="Buscar">
      </div>
      <a class="descarga" href="/leads.csv">↓ Descargar CSV</a>
      <div class="wrap"><table><tr><th>Persona</th><th>Créditos</th><th>Desde</th><th>Feedback</th></tr>{filas}</table></div>"""
    return page("Usuarios · TΛLENO OS", contenido, user, "leads", LEADS_CSS, LEADS_JS)


HIST_CSS = """
  .hist { display: grid; gap: 12px; }
  .hist a.fila { background: var(--papel); border: 1px solid var(--linea); border-radius: 14px;
    padding: 16px 18px; text-decoration: none; display: flex; flex-direction: column; gap: 4px; }
  .hist a.fila:hover { border-color: var(--naranja); }
  .hist .tit { font-weight: 700; font-size: 17px; }
  .hist .sub { color: var(--gris); font-size: 13px; }
  .pill { display: inline-block; font-size: 11px; font-weight: 700; letter-spacing: .06em;
    text-transform: uppercase; padding: 3px 9px; border-radius: 999px; margin-right: 8px; }
  .pill.radar { background: #e8f1ff; color: #1d4ed8; }
  .pill.mercado { background: #fff1e9; color: #b8430f; }
  .barra-detalle { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }
  .borrar { background: none; border: 1.5px solid var(--linea); color: var(--gris); border-radius: 10px;
    padding: 9px 14px; font: 600 14px inherit; cursor: pointer; }
  .borrar:hover { border-color: var(--no); color: var(--no); }
"""

HIST_JS = """
const borrar = document.getElementById("borrarAnalisis");
if (borrar) borrar.addEventListener("click", async () => {
  if (!confirm("¿Borrar este análisis del historial? No se puede deshacer.")) return;
  const res = await fetch("/api/historial/" + borrar.dataset.id, { method: "DELETE" });
  if (res.ok) window.location.href = "/historial";
});
"""


def historial_page(user: str) -> str:
    filas_db = historial_listar(user)
    if not USA_SUPABASE:
        cuerpo = '<p class="empty">El historial necesita Supabase configurado.</p>'
    elif not filas_db:
        detalle = _sb_ultimo_error["detalle"]
        extra = f'<p class="hint">Diagnóstico: {detalle}</p>' if (detalle and not is_guest(user)) else ""
        cuerpo = '<p class="empty">Todavía no has hecho ningún análisis. Empieza por el Radar.</p>' + extra
    else:
        tarjetas = ""
        for f in filas_db:
            agente = "mercado" if f.get("agente") == "mercado" else "radar"
            nombre_agente = "Analista" if agente == "mercado" else "Radar"
            fecha = (f.get("creado") or "")[:16].replace("T", " ")
            tarjetas += f"""<a class="fila" href="/historial/{f.get('id')}">
              <span class="tit">{(f.get('titulo') or 'Análisis')}</span>
              <span class="sub"><span class="pill {agente}">{nombre_agente}</span>
                {f.get('total_comentarios', 0)} comentarios · {f.get('fuente','')} · {fecha}</span></a>"""
        cuerpo = f'<div class="hist">{tarjetas}</div>'
    contenido = f"""<h1>Historial</h1>
      <p class="intro">Todos tus análisis guardados. Ábrelos cuando quieras, desde cualquier dispositivo.</p>
      {cuerpo}"""
    return page("Historial · TΛLENO OS", contenido, user, "historial", RESULT_CSS + HIST_CSS)


def _json_para_script(datos) -> str:
    """JSON seguro dentro de <script>: un comentario con </script> rompía la página."""
    crudo = json.dumps(datos, ensure_ascii=False)
    return (crudo.replace("<", "\\u003c").replace(">", "\\u003e")
                 .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029"))


def historial_detalle_page(user: str, fila: dict) -> str:
    agente = "Analista de Mercado" if fila.get("agente") == "mercado" else "Radar"
    fecha = (fila.get("creado") or "")[:16].replace("T", " ")
    ajeno = fila.get("usuario") and fila.get("usuario") != user
    volver = ('<a class="tab" href="/actividad">← Volver a actividad</a>'
              if ajeno else '<a class="tab" href="/historial">← Volver al historial</a>')
    de_quien = (f' · de {fila.get("usuario", "").replace(GUEST_PREFIX, "")}' if ajeno else "")
    contenido = f"""<h1>{fila.get('titulo') or 'Análisis'}</h1>
      <p class="intro">{agente} · {fila.get('total_comentarios', 0)} comentarios · {fila.get('fuente','')} · {fecha}{de_quien}</p>
      <div class="barra-detalle">
        {volver}
        <button type="button" class="borrar" id="borrarAnalisis" data-id="{fila.get('id')}">Borrar</button>
      </div>
      <div id="results"></div>
      <p class="pie-impresion">© {ANIO} Richard Taleno · Todos los derechos reservados · Generado con TΛLENO OS · Consultas: {CONTACT_EMAIL}</p>"""
    datos_dict = dict(fila.get("datos") or {})
    datos_dict["id_analisis"] = fila.get("id")
    datos = _json_para_script(datos_dict)
    script = RENDER_JS.replace("__CONTACTO__", PIE_TEXTO) + f"""
const DATOS = {datos};
el("results").innerHTML = pintarSegunAgente(DATOS);
conectarBotones(DATOS);
""" + HIST_JS
    return page("Análisis · TΛLENO OS", contenido, user, "historial", RESULT_CSS + HIST_CSS, script)


COPY_JS = """
const PAQUETE_OK = __PAQUETE__;

function opcionesFormato() { return el("formato").value; }

el("form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const id = el("analisis").value;
  if (!id) { el("status").className = "status error"; el("status").textContent = "Elige un análisis primero."; return; }
  el("status").className = "status";
  el("status").innerHTML = '<span class="spinner"></span>Escribiendo el contenido.';
  el("btn").disabled = true; el("btn").textContent = "Escribiendo…";
  try {
    const cuerpo = {
      analisis_id: id, formato: el("formato").value,
      angulo_a: el("anguloA").value, angulo_b: el("anguloB").value,
      estado: el("estado").value, paquete: PAQUETE_OK && el("paquete").checked,
    };
    const res = await fetch("/copy", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(cuerpo) });
    if (res.status === 401) { window.location.href = "/login"; return; }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : "No se pudo escribir el contenido.");
    el("status").textContent = "";
    el("results").innerHTML = renderCopy(data);
    conectarBotones(data);
    el("results").scrollIntoView({ behavior: "smooth", block: "start" });
    el("fbBox").hidden = false;
  } catch (err) {
    el("status").className = "status error";
    el("status").textContent = "No se pudo escribir: " + err.message;
  } finally {
    el("btn").disabled = false; el("btn").textContent = "Escribir contenido";
  }
});

let fbUtil = null;
document.querySelectorAll(".fbrow .tab").forEach(b => b.addEventListener("click", () => {
  fbUtil = b.dataset.util === "1";
  document.querySelectorAll(".fbrow .tab").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
}));
el("fbEnviar").addEventListener("click", async () => {
  const s = el("fbStatus");
  try {
    await fetch("/api/feedback", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ util: fbUtil, texto: el("fbTexto").value }) });
    s.className = "status"; s.textContent = "¡Gracias! Lo leo todo.";
    el("fbTexto").value = "";
  } catch (err) { s.className = "status error"; s.textContent = "No se pudo enviar."; }
});

// Si venimos de un informe, lo dejamos seleccionado
const params = new URLSearchParams(window.location.search);
const pre = params.get("analisis");
if (pre) { const sel = el("analisis"); if ([...sel.options].some(o => o.value === pre)) sel.value = pre; }
"""


def copy_page(user: str) -> str:
    c = CEREBROS["copy"]
    if is_guest(user) and creditos_de(guest_email(user)) == 0:
        cuerpo = (f"<h1>{c['nombre']}</h1><p class=\"intro\">{c['desc']}</p>"
                  + panel_sin_creditos("El agente Copy")
                  + '<p style="margin-top:18px"><a class="tab" href="/cerebro/radar">← Volver al Radar</a></p>')
        return page("Copy · TΛLENO OS", cuerpo, user, "copy", RESULT_CSS)
    informes = [f for f in historial_listar(user, 30) if f.get("agente") == "mercado"]
    if not informes:
        aviso = ('<p class="empty">Todavía no tienes informes del Analista de Mercado. '
                 'Haz uno primero: el Copy escribe a partir de ese informe.</p>'
                 '<p><a class="tab" href="/cerebro/analista">Ir al Analista</a></p>')
        return page("Copy · TΛLENO OS", f"<h1>{c['nombre']}</h1><p class=\"intro\">{c['desc']}</p>{aviso}",
                    user, "copy", RESULT_CSS)

    opciones = "".join(
        f'<option value="{f.get("id")}">{(f.get("titulo") or "Análisis")} · {(f.get("creado") or "")[:10]}</option>'
        for f in informes)
    sel_formato = "".join(f'<option value="{k}">{v}</option>' for k, v in FORMATOS_COPY.items())
    sel_a = "".join(f'<option value="{k}">{v}</option>' for k, v in ANGULOS_A.items())
    sel_b = "".join(f'<option value="{k}">{v}</option>' for k, v in ANGULOS_B.items())
    sel_estado = "".join(f'<option value="{k}">{v}</option>' for k, v in ESTADOS_PRODUCTO.items())
    puede_paquete = not is_guest(user)
    paquete_html = ('<label class="check"><input type="checkbox" id="paquete"> '
                    'Generar el paquete: una pieza de cada formato</label>'
                    if puede_paquete else
                    '<p class="hint">🔒 El paquete de 5 formatos está disponible para cuentas.</p>'
                    '<input type="checkbox" id="paquete" hidden>')
    limite = ('<p class="hint">Tienes 1 pieza de prueba con este agente.</p>' if is_guest(user) else "")

    contenido = f"""<h1>{c['nombre']}</h1>
      <p class="intro">{c['desc']}</p>
      {limite}
      <form id="form">
        <div class="label">Informe de base</div>
        <select id="analisis" aria-label="Análisis">{opciones}</select>
        <div class="row" style="margin-top:12px">
          <div><div class="label">Formato</div><select id="formato" aria-label="Formato">{sel_formato}</select></div>
          <div><div class="label">Estado del producto</div><select id="estado" aria-label="Estado">{sel_estado}</select></div>
        </div>
        <div class="row" style="margin-top:12px">
          <div><div class="label">Qué ataca</div><select id="anguloA" aria-label="Ángulo A">{sel_a}</select></div>
          <div><div class="label">Cómo lo cuenta</div><select id="anguloB" aria-label="Ángulo B">{sel_b}</select></div>
        </div>
        <div style="margin-top:12px">{paquete_html}</div>
        <button id="btn" class="submit" type="submit">Escribir contenido</button>
      </form>
      <p id="status" class="status" role="status"></p>
      <div id="results"></div>
      <p class="pie-impresion">© {ANIO} Richard Taleno · Todos los derechos reservados · Generado con TΛLENO OS · Consultas: {CONTACT_EMAIL}</p>
      <div id="fbBox" class="fbbox" hidden>
        <strong>¿Te sirvió este contenido?</strong>
        <div class="fbrow">
          <button type="button" class="tab" data-util="1">👍 Sí</button>
          <button type="button" class="tab" data-util="0">👎 No</button>
        </div>
        <textarea id="fbTexto" placeholder="¿Qué le falta? ¿Qué cambiarías? (opcional)"></textarea>
        <button type="button" id="fbEnviar" class="submit">Enviar comentario</button>
        <p id="fbStatus" class="status"></p>
      </div>"""
    script = (RENDER_JS.replace("__CONTACTO__", PIE_TEXTO) +
              COPY_JS.replace("__PAQUETE__", "true" if puede_paquete else "false"))
    return page("Copy · TΛLENO OS", contenido, user, "copy", RESULT_CSS, script)


ACT_CSS = """
  .kpis { display: grid; gap: 12px; grid-template-columns: repeat(2, 1fr); margin-bottom: 22px; }
  @media (min-width: 720px) { .kpis { grid-template-columns: repeat(4, 1fr); } }
  .kpi { background: var(--papel); border: 1px solid var(--linea); border-radius: 14px; padding: 16px; }
  .kpi .n { font-size: 26px; font-weight: 700; }
  .kpi .t { color: var(--gris); font-size: 13px; }
  .tema { display: flex; justify-content: space-between; gap: 12px; padding: 10px 0;
    border-bottom: 1px solid var(--linea); font-size: 15px; }
  .tema span:last-child { color: var(--gris); white-space: nowrap; }
"""


def actividad_page(user: str) -> str:
    filas = actividad_listar()
    if not USA_SUPABASE:
        return page("Actividad · TΛLENO OS", "<h1>Actividad</h1><p class=\"empty\">Necesita Supabase configurado.</p>",
                    user, "actividad", RESULT_CSS + ACT_CSS)

    usuarios = {}
    por_agente = {}
    for f in filas:
        u = f.get("usuario", "")
        usuarios[u] = usuarios.get(u, 0) + 1
        por_agente[f.get("agente", "?")] = por_agente.get(f.get("agente", "?"), 0) + 1

    nombres = {l.get("email"): l.get("nombre") for l in _read_leads().values()}

    def quien(u: str) -> str:
        if u.startswith(GUEST_PREFIX):
            correo = u[len(GUEST_PREFIX):]
            return f'{nombres.get(correo) or correo} <span class="fb">· invitado</span>'
        return f'{u} <span class="fb">· cuenta</span>'

    kpis = f"""<div class="kpis">
      <div class="kpi"><div class="n">{len(filas)}</div><div class="t">Análisis</div></div>
      <div class="kpi"><div class="n">{len(usuarios)}</div><div class="t">Personas</div></div>
      <div class="kpi"><div class="n">{por_agente.get('rapido', 0)}</div><div class="t">Radar</div></div>
      <div class="kpi"><div class="n">{por_agente.get('mercado', 0)}</div><div class="t">Analista</div></div>
    </div>"""

    temas = ""
    for f in filas[:25]:
        temas += f"""<div class="tema"><a href="/historial/{f.get('id')}">{f.get('titulo') or 'Análisis'}</a>
          <span>{(f.get('creado') or '')[:10]}</span></div>"""

    tabla = ""
    for f in filas[:100]:
        agente = {"rapido": "Radar", "mercado": "Analista", "copy": "Copy"}.get(f.get("agente"), f.get("agente"))
        tabla += f"""<tr><td>{quien(f.get('usuario',''))}</td><td>{agente}</td>
          <td><a href="/historial/{f.get('id')}">{f.get('titulo') or 'Análisis'}</a><br>
          <span class="fb">{f.get('fuente','')} · {f.get('total_comentarios',0)} comentarios</span></td>
          <td>{(f.get('creado') or '')[:16].replace('T', ' ')}</td></tr>"""
    if not tabla:
        tabla = '<tr><td colspan="4">Todavía no hay análisis.</td></tr>'

    contenido = f"""<h1>Actividad</h1>
      <p class="intro">Qué está buscando la gente que usa tus agentes. Toca cualquier título para abrir el informe completo.</p>
      {kpis}
      <a class="descarga" href="/actividad.csv">↓ Descargar CSV</a>
      <h2>Últimos temas analizados</h2>
      <div>{temas or '<p class="empty">Sin datos todavía.</p>'}</div>
      <h2>Detalle</h2>
      <div class="wrap"><table>
        <tr><th>Persona</th><th>Agente</th><th>Análisis</th><th>Fecha</th></tr>{tabla}</table></div>"""
    return page("Actividad · TΛLENO OS", contenido, user, "actividad", RESULT_CSS + LEADS_CSS + ACT_CSS)


NOV_CSS = """
  .novedad { background: var(--papel); border: 1px solid var(--linea); border-radius: 14px;
    padding: 20px 22px; margin-bottom: 14px; }
  .novedad h3 { margin: 0 0 4px; font-size: 18px; }
  .novedad .fecha { color: var(--gris); font-size: 13px; }
  .novedad .cuerpo { margin-top: 10px; white-space: pre-wrap; }
  .novedad .quitar { float: right; background: none; border: 0; color: var(--gris); cursor: pointer; font-size: 13px; }
  .novedad .quitar:hover { color: var(--no); }
  .caja-nueva { background: var(--papel); border: 1px dashed var(--linea); border-radius: 14px;
    padding: 20px 22px; margin-bottom: 24px; }
  .caja-nueva textarea { min-height: 120px; }
  .punto { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    background: var(--naranja); margin-left: 6px; vertical-align: middle; }
"""

NOV_JS = """
try { localStorage.setItem("novedades_vista", document.body.dataset.ultimaNovedad || ""); } catch (e) {}

try {
  const b = sessionStorage.getItem("borrador_novedad");
  if (b && document.getElementById("novTitulo")) {
    const { titulo, texto } = JSON.parse(b);
    sessionStorage.removeItem("borrador_novedad");
    document.getElementById("novTitulo").value = titulo || "";
    document.getElementById("novTexto").value = texto || "";
    document.getElementById("novTitulo").scrollIntoView({ behavior: "smooth", block: "center" });
  }
} catch (e) {}

const formNov = document.getElementById("formNovedad");
if (formNov) formNov.addEventListener("submit", async (e) => {
  e.preventDefault();
  const s = document.getElementById("novStatus");
  s.className = "status"; s.innerHTML = '<span class="spinner"></span>Publicando…';
  try {
    const res = await fetch("/api/novedades", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ titulo: document.getElementById("novTitulo").value.trim(),
                             texto: document.getElementById("novTexto").value.trim() }) });
    if (!res.ok) throw new Error("No se pudo publicar.");
    window.location.reload();
  } catch (err) { s.className = "status error"; s.textContent = err.message; }
});

document.querySelectorAll(".quitar").forEach(b => b.addEventListener("click", async () => {
  if (!confirm("¿Borrar esta novedad?")) return;
  const res = await fetch("/api/novedades/" + b.dataset.id, { method: "DELETE" });
  if (res.ok) window.location.reload();
}));
"""


def novedades_page(user: str) -> str:
    filas = novedades_listar()
    admin = not is_guest(user)

    formulario = ""
    if admin:
        formulario = """<div class="caja-nueva">
          <div class="label">Publicar una novedad</div>
          <form id="formNovedad">
            <div class="campo" style="margin-bottom:10px">
              <input id="novTitulo" maxlength="150" required
                placeholder="Ej: Ya puedes convertir un informe en anuncios y posts" aria-label="Título"></div>
            <textarea id="novTexto" required maxlength="4000"
              placeholder="Cuéntalo en términos de lo que la persona puede hacer ahora."></textarea>
            <button class="submit" type="submit">Publicar</button>
          </form>
          <p id="novStatus" class="status"></p>
        </div>"""

    lista = ""
    for n in filas:
        quitar = (f'<button class="quitar" data-id="{n.get("id")}">Borrar</button>' if admin else "")
        texto = (n.get("texto") or "").replace("<", "&lt;")
        lista += f"""<div class="novedad">{quitar}
          <h3>{(n.get('titulo') or '').replace('<', '&lt;')}</h3>
          <div class="fecha">{(n.get('creado') or '')[:10]}</div>
          <div class="cuerpo">{texto}</div></div>"""
    if not lista:
        lista = '<p class="empty">Todavía no hay novedades publicadas.</p>'

    contenido = f"""<h1>Novedades</h1>
      <p class="intro">Lo último que se ha sumado a TΛLENO OS.</p>
      {formulario}{lista}"""
    return page("Novedades · TΛLENO OS", contenido, user, "novedades", NOV_CSS, NOV_JS)


NICHOS_CSS = """
  .destacado { background: var(--papel); border: 1px solid var(--linea); border-radius: 16px;
    padding: 18px 20px; margin-bottom: 14px; }
  .destacado h2 { font-size: 16px; margin: 0 0 10px; }
  .fila-nicho { display: flex; justify-content: space-between; gap: 12px; align-items: baseline;
    padding: 9px 0; border-bottom: 1px solid var(--linea); }
  .fila-nicho:last-child { border-bottom: 0; }
  .fila-nicho .n { font-weight: 600; }
  .fila-nicho .c { color: var(--gris); font-size: 13px; white-space: nowrap; }
  .nicho { background: var(--papel); border: 1px solid var(--linea); border-radius: 16px;
    padding: 18px 20px; margin-bottom: 12px; }
  .nicho.caliente { border-color: var(--naranja); }
  .nicho .cab { display: flex; justify-content: space-between; gap: 10px; align-items: baseline; flex-wrap: wrap; }
  .nicho .tema { font-size: 18px; font-weight: 700; }
  .nicho .meta { color: var(--gris); font-size: 13px; margin-top: 4px; }
  .nicho .quien { color: var(--gris); font-size: 13px; margin-top: 10px; }
  .nicho .quien a { color: var(--azul); text-decoration: none; }
  .nicho .acciones { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 14px; }
  .sello { font-size: 11px; font-weight: 700; letter-spacing: .06em; text-transform: uppercase;
    color: var(--naranja); background: #fff1e9; border-radius: 999px; padding: 4px 10px; }
  .vacio { color: var(--gris); font-size: 14px; }
"""

NICHOS_JS = """
document.querySelectorAll(".escribir").forEach(b => b.addEventListener("click", () => {
  try {
    sessionStorage.setItem("borrador_novedad", JSON.stringify({
      titulo: b.dataset.titulo, texto: b.dataset.texto }));
  } catch (e) {}
  window.location.href = "/novedades";
}));
"""


def _tema_limpio(etiqueta: str) -> str:
    """Quita del tema lo que es del video y no del nicho: años, paréntesis, precios, emojis."""
    t = re.sub(r"\([^)]*\)", " ", etiqueta or "")          # (2026), (parte 2)...
    t = re.sub(r"\b(19|20)\d{2}\b", " ", t)                 # años sueltos
    t = re.sub(r"(con\s+)?(solo\s+)?[$€]\s?\d[\d.,]*\s*(usd|dólares|dolares)?", " ", t, flags=re.I)  # precios
    t = re.sub(r"[|!¡?¿#*_\"]+", " ", t)
    t = re.sub(r"\s{2,}", " ", t).strip(" -–—.,:")
    t = re.sub(r"^(c[oó]mo|que|qu[eé]|ideas para|tips para|gu[ií]a de|el|la|los|las)\s+", "", t, flags=re.I)
    return (t[:1].lower() + t[1:]) if t else (etiqueta or "este tema")


# La primera es para los nichos que buscó más de una persona; la segunda, para los de una sola.
PLANTILLAS_NOVEDAD = [
    ("Un buen tema para probar esta semana: {tema}",
     "Si {tema} es tu nicho, o si te interesa, corre el Radar sobre ese tema. "
     "Vas a ver los comentarios reales ordenados en dolores, objeciones y deseos, con las frases textuales.\n\n"
     "Después pásalos al Analista y sabrás si ahí hay un producto que valga la pena crear, "
     "o si conviene buscar en otro lado."),
    ("Cómo investigar {tema} en 5 minutos",
     "1. Escribe el tema en el Radar y elige 5 videos.\n"
     "2. Lee los dolores y quédate con la frase que más se repite.\n"
     "3. Manda los comentarios al Analista para saber si ahí hay un producto.\n\n"
     "No necesitas encuestar a nadie: tu mercado ya lo escribió."),
]


def nichos_page(user: str) -> str:
    filas = material_listar(120)
    if not USA_SUPABASE:
        return page("Nichos · TΛLENO OS", "<h1>Nichos</h1><p class=\"empty\">Necesita Supabase configurado.</p>",
                    user, "nichos", NICHOS_CSS)

    grupos = agrupar_nichos(filas)
    nombres = {l.get("email"): l.get("nombre") for l in _read_leads().values()}
    hace_7 = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    def persona(u: str) -> str:
        correo = u.replace(GUEST_PREFIX, "")
        return nombres.get(correo) or correo.split("@")[0]

    # --- tres listas cortas arriba ---
    repetidos = [g for g in grupos if len(g["personas"]) > 1][:5]
    nuevos = [g for g in grupos if g["primera"] >= hace_7][:5]
    sin_analista = [g for g in grupos if "mercado" not in g["agentes"]][:5]

    def mini(lista, texto_derecha):
        if not lista:
            return '<p class="vacio">Todavía nada por aquí.</p>'
        return "".join(f'<div class="fila-nicho"><span class="n">{g["etiqueta"]}</span>'
                       f'<span class="c">{texto_derecha(g)}</span></div>' for g in lista)

    arriba = f"""<div class="destacado"><h2>🔁 Buscado por varias personas</h2>
        {mini(repetidos, lambda g: f'{len(g["personas"])} personas · {len(g["analisis"])} análisis')}</div>
      <div class="destacado"><h2>✨ Nuevos esta semana</h2>
        {mini(nuevos, lambda g: (g["primera"] or "")[:10])}</div>
      <div class="destacado"><h2>◎ Se quedaron en el Radar</h2>
        {mini(sin_analista, lambda g: f'{len(g["analisis"])} análisis')}</div>"""

    # --- detalle por nicho ---
    tarjetas = ""
    for g in grupos[:30]:
        caliente = len(g["personas"]) > 1
        sello = '<span class="sello">Varias personas</span>' if caliente else ""
        agentes = ", ".join({"rapido": "Radar", "mercado": "Analista"}.get(a, a) for a in sorted(g["agentes"]))
        quien = " · ".join(
            f'<a href="/historial/{f.get("id")}">{persona(f.get("usuario",""))}, {(f.get("creado") or "")[:10]}</a>'
            for f in g["analisis"][:6])

        tema = _tema_limpio(g["etiqueta"])
        plantilla = PLANTILLAS_NOVEDAD[0 if caliente else 1]
        titulo_nov = plantilla[0].format(tema=tema, Tema=tema[:1].upper() + tema[1:])
        texto_nov = plantilla[1].format(tema=tema, Tema=tema[:1].upper() + tema[1:])

        tarjetas += f"""<div class="nicho{' caliente' if caliente else ''}">
          <div class="cab"><span class="tema">{g['etiqueta']}</span>{sello}</div>
          <div class="meta">{len(g['analisis'])} análisis · {len(g['personas'])} personas · {agentes} · último {(g['ultima'] or '')[:10]}</div>
          <div class="quien">{quien}</div>
          <div class="acciones">
            <button type="button" class="tab escribir"
              data-titulo="{titulo_nov.replace(chr(34), '&quot;')}"
              data-texto="{texto_nov.replace(chr(34), '&quot;')}">★ Escribir novedad</button>
          </div></div>"""

    if not tarjetas:
        tarjetas = '<p class="empty">Todavía no hay análisis suficientes.</p>'

    contenido = f"""<h1>Nichos</h1>
      <p class="intro">Qué temas está investigando la gente en tus agentes. Lo que aparece en varias personas
        es tema de publicación; lo que aparece una vez, todavía no.</p>
      {arriba}
      <h2>Todos los nichos</h2>
      {tarjetas}"""
    return page("Nichos · TΛLENO OS", contenido, user, "nichos", NICHOS_CSS, NICHOS_JS)


def panel_sin_creditos(agente: str) -> str:
    boton = (f'<a class="submit" style="display:inline-block;text-decoration:none" href="{VENTA_URL}" '
             f'target="_blank" rel="noopener">Ver paquetes</a>' if VENTA_URL else "")
    return f"""<div class="sin-creditos">
      <h2>Se te acabaron los créditos</h2>
      <p>El <strong>Radar sigue gratis y sin límite</strong>: puedes seguir trayendo comentarios de YouTube
         y ordenándolos en dolores, objeciones y deseos.</p>
      <p>{agente} necesita créditos. Con ellos conviertes esos comentarios en una decisión
         y en contenido listo para publicar.</p>
      {boton}
      <p class="letra-chica">Una investigación completa (Analista + Copy) usa 2 créditos.</p>
    </div>"""


def brain_page(slug: str, user: str) -> str:
    if slug == "copy":
        return copy_page(user)
    c = CEREBROS[slug]
    if (is_guest(user) and COSTO_CREDITOS.get(c["modo"], 1) > 0
            and creditos_de(guest_email(user)) == 0):
        cuerpo = (f"<h1>{c['nombre']}</h1><p class=\"intro\">{c['desc']}</p>"
                  + panel_sin_creditos("El Analista de Mercado")
                  + '<p style="margin-top:18px"><a class="tab" href="/cerebro/radar">← Volver al Radar</a></p>')
        return page(f"{c['nombre']} · TΛLENO OS", cuerpo, user, slug, RESULT_CSS)
    etiquetas = {
        "paste": "Pegar comentarios",
        "yt-search": "Buscar en YouTube",
        "yt-video": "Video de YouTube",
        "facebook": "Facebook",
    }
    fuentes = list(c.get("fuentes", ["paste"]))
    bloqueadas = ["facebook"] if (is_guest(user) and not GUEST_FULL and "facebook" in fuentes) else []
    primera = next(f for f in fuentes if f not in bloqueadas)
    if len(fuentes) > 1:
        botones = "".join(
            f'<button class="tab{" bloqueada" if f in bloqueadas else ""}" data-src="{f}" '
            f'aria-pressed="{"true" if f == primera else "false"}">'
            f'{"🔒 " if f in bloqueadas else ""}{etiquetas[f]}</button>'
            for f in fuentes)
        selector = f'<div class="label">Fuente de los comentarios</div><div class="tabs" id="sources">{botones}</div>'
    else:
        selector = f'<div class="tabs" id="sources" hidden><button class="tab" data-src="{primera}" aria-pressed="true"></button></div>'

    contenido = f"""<h1>{c['nombre']}</h1>
      <p class="intro">{c['desc']}</p>
      {selector}
      <p id="hint" class="hint"></p>

      <form id="form">
        <div id="pasteBox">
          <textarea id="texto" placeholder="Pega aquí los comentarios, uno por línea (YouTube, TikTok, Reddit, foros...)"></textarea>
          <div id="counter" class="counter">0 comentarios detectados</div>
        </div>
        <div class="row" id="lineBox" hidden>
          <input id="field" aria-label="Entrada">
          <select id="nvideos" aria-label="Cantidad de videos">
            <option value="3">3 videos</option><option value="5" selected>5 videos</option><option value="8">8 videos</option>
          </select>
        </div>
        <div class="row" style="margin-top:10px">
          <input id="nicho" maxlength="150" placeholder="Nicho o tema (opcional)" aria-label="Nicho">
        </div>
        <button id="btn" class="submit" type="submit">Analizar</button>
      </form>
      <p id="status" class="status" role="status"></p>
      <div id="results"></div>
      <p class="pie-impresion">© {ANIO} Richard Taleno · Todos los derechos reservados · Generado con TΛLENO OS · Consultas: {CONTACT_EMAIL}</p>
      <div id="fbBox" class="fbbox" hidden>
        <strong>¿Te sirvió este análisis?</strong>
        <div class="fbrow">
          <button type="button" class="tab" data-util="1">👍 Sí</button>
          <button type="button" class="tab" data-util="0">👎 No</button>
        </div>
        <textarea id="fbTexto" placeholder="¿Qué le falta? ¿Qué cambiarías? (opcional)"></textarea>
        <button type="button" id="fbEnviar" class="submit">Enviar comentario</button>
        <p id="fbStatus" class="status"></p>
      </div>"""
    script = RENDER_JS.replace("__CONTACTO__", PIE_TEXTO) + (BRAIN_JS.replace("__MODO__", c["modo"]).replace("__INICIAL__", primera)
                          .replace("__BLOQUEADAS__", json.dumps(bloqueadas)))
    return page(f"{c['nombre']} · TΛLENO OS", contenido, user, slug, RESULT_CSS, script)


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
class LoginRequest(BaseModel):
    email: str
    password: str


def _titulo_informe(modo: str, result: dict, titulo: Optional[str]) -> str:
    """Para el Analista, el título sale del propio informe, no del número de comentarios."""
    if modo == "mercado":
        informe = result.get("mercado")
        if informe is not None:
            datos = informe.model_dump() if hasattr(informe, "model_dump") else informe
            producto = (datos.get("propuesta_producto") or {}).get("nombre", "").strip()
            problema = (datos.get("problema_urgente") or {}).get("problema_urgente_especifico", "").strip()
            if producto and problema:
                return f"{producto} — {problema[:90]}"
            if producto or problema:
                return (producto or problema)[:140]
    return titulo or "Análisis"


async def build_response(fuente, consulta, comments, modo, result, user,
                         videos=None, titulo=None) -> AnalyzeResponse:
    restantes = None
    if is_guest(user):
        cuantos = COSTO_CREDITOS.get(modo, 1)
        restantes = await consumir_credito(guest_email(user), cuantos) if cuantos else creditos_de(guest_email(user))
    respuesta = AnalyzeResponse(
        fuente=fuente, consulta=consulta, total_comentarios=len(comments),
        modelo=GEMINI_MODEL, modo=modo, videos=videos or [], restantes=restantes,
        comentarios=comments if modo == "rapido" else [], **result,
    )
    try:
        respuesta.id_analisis = historial_guardar(
            usuario=user, agente=modo, titulo=_titulo_informe(modo, result, titulo or consulta),
            fuente=fuente, total=len(comments), datos=respuesta.model_dump(mode="json"),
        )
    except Exception:
        logger.exception("No se pudo guardar en el historial")
    return respuesta


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {
        "status": "ok", "version": APP_VERSION, "modelo": GEMINI_MODEL,
        "facebook": bool(APIFY_API_TOKEN), "youtube": bool(YOUTUBE_API_KEY),
        "usuarios_configurados": len(USERS), "modo_invitado": GUEST_MODE,
        "almacen": "supabase" if USA_SUPABASE else "archivo local",
        "systeme": bool(SYSTEME_API_KEY),
    }


@app.get("/login", response_class=HTMLResponse)
async def login_view(request: Request):
    if current_user(request):
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')
    if not USERS:
        return HTMLResponse(login_page("Falta configurar APP_USERS en las variables de entorno."))
    return HTMLResponse(login_page())


@app.post("/api/login")
async def login_api(req: LoginRequest, response: Response):
    email = req.email.strip().lower()

    esperado = USERS.get(email)                      # cuentas de administración
    if esperado and secrets.compare_digest(esperado, req.password):
        response.set_cookie(
            COOKIE_NAME, signer.dumps(email), max_age=SESSION_DAYS * 86400,
            httponly=True, samesite="lax", secure=COOKIE_SECURE,
        )
        return {"ok": True}

    lead = lead_get(email)                           # cuentas de usuario
    if lead and lead.get("pass_hash") and clave_correcta(req.password, lead["pass_hash"]):
        _abrir_sesion(response, email)
        return {"ok": True}

    if lead and not lead.get("pass_hash"):
        raise HTTPException(
            status_code=401,
            detail="Tu cuenta es anterior a las contraseñas. Escríbeme y te genero una.",
        )
    raise HTTPException(status_code=401, detail="Correo o contraseña incorrectos.")


class GuestRequest(BaseModel):
    nombre: str = Field(..., min_length=2, max_length=80)
    email: str = Field(..., max_length=120)


class FeedbackRequest(BaseModel):
    util: Optional[bool] = None
    texto: str = Field("", max_length=1000)


def _abrir_sesion(response: Response, email: str):
    response.set_cookie(
        COOKIE_NAME, signer.dumps(GUEST_PREFIX + email), max_age=SESSION_DAYS * 86400,
        httponly=True, samesite="lax", secure=COOKIE_SECURE,
    )


class RegistroRequest(BaseModel):
    nombre: str = Field(..., min_length=2, max_length=80)
    email: str = Field(..., max_length=120)
    password: str = Field(..., min_length=8, max_length=100)


@app.post("/api/registro")
async def crear_cuenta(req: RegistroRequest, response: Response):
    if not GUEST_MODE:
        raise HTTPException(status_code=403, detail="El registro está cerrado por ahora.")
    email = req.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Escribe un correo válido.")
    if email in USERS:
        raise HTTPException(status_code=400, detail="Ese correo ya tiene cuenta. Entra con tu contraseña.")

    existente = lead_get(email)
    if existente and existente.get("pass_hash"):
        raise HTTPException(status_code=400, detail="Ese correo ya tiene cuenta. Entra con tu contraseña.")

    await lead_guardar(email, req.nombre.strip())
    await guardar_clave(email, req.password)
    if not existente:
        info = await systeme_sync(email, req.nombre.strip())
        await lead_marcar_systeme(email, info.get("contacto", False))

    _abrir_sesion(response, email)
    return {"ok": True}


class RecuperarRequest(BaseModel):
    email: str = Field(..., max_length=120)


class NuevaClaveRequest(BaseModel):
    token: str
    password: str = Field(..., min_length=8, max_length=100)


@app.post("/api/recuperar")
async def pedir_recuperacion(req: RecuperarRequest):
    email = req.email.strip().lower()
    if not USA_CORREO:
        raise HTTPException(status_code=503, detail="Escríbeme a richard@richardtaleno.com y te genero una contraseña nueva.")
    if EMAIL_RE.match(email):
        await enviar_recuperacion(email)          # si no existe, no decimos nada
    return {"ok": True}


@app.get("/recuperar", response_class=HTMLResponse)
async def pagina_recuperar(t: str = ""):
    email = await validar_recuperacion(t) if t else None
    if not email:
        return HTMLResponse(login_page("Ese enlace ya se usó o venció. Pide otro."), status_code=400)
    return HTMLResponse(nueva_clave_page(t, email))


@app.post("/api/recuperar/confirmar")
async def confirmar_recuperacion(req: NuevaClaveRequest, response: Response):
    email = await validar_recuperacion(req.token)
    if not email:
        raise HTTPException(status_code=400, detail="Ese enlace ya se usó o venció. Pide otro.")
    await guardar_clave(email, req.password)
    await guardar_nonce(email, None)
    _abrir_sesion(response, email)
    return {"ok": True}


@app.post("/api/feedback")
async def enviar_feedback(req: FeedbackRequest, user: str = Depends(require_user)):
    if is_guest(user):
        await lead_feedback(guest_email(user), req.util, req.texto.strip())
    else:
        logger.info("Feedback de %s: %s %s", user, req.util, req.texto[:200])
    return {"ok": True}


@app.get("/supabase/test")
async def supabase_test(request: Request):
    """Diagnóstico: escribe y lee un registro de prueba en la tabla leads."""
    user = current_user(request)
    if not user or is_guest(user):
        raise HTTPException(status_code=401, detail="Necesitas entrar con tu cuenta.")
    if not USA_SUPABASE:
        return {"almacen": "archivo local", "detalle": "Faltan SUPABASE_URL o SUPABASE_KEY en Render."}

    correo = "prueba-diagnostico@taleno.test"
    lead = _lead_nuevo(correo, "Prueba")
    lead["feedback"] = [{"util": True, "texto": "escritura de prueba", "fecha": lead["creado"]}]
    _lead_upsert(lead)
    error_escritura = _sb_ultimo_error["detalle"]
    leido = lead_get(correo)
    return {
        "almacen": "supabase",
        "url_usada": _sb_url(),
        "url_configurada": (SUPABASE_URL or "").strip(),
        "largo_de_la_clave": len(SUPABASE_KEY or ""),
        "escritura_ok": not error_escritura,
        "error_escritura": error_escritura,
        "lectura_ok": bool(leido),
        "feedback_leido": (leido or {}).get("feedback"),
        "ultimo_error": _sb_ultimo_error["detalle"],
        "total_filas": len(_read_leads()),
    }


@app.get("/systeme/tags")
async def systeme_tags(request: Request):
    """Lista tus tags de systeme.io con su id, para copiar el correcto."""
    user = current_user(request)
    if not user or is_guest(user):
        raise HTTPException(status_code=401, detail="Necesitas entrar con tu cuenta.")
    if not SYSTEME_API_KEY:
        raise HTTPException(status_code=400, detail="Falta SYSTEME_API_KEY.")
    headers = {"X-API-Key": SYSTEME_API_KEY}
    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        resp = await client.get(f"{SYSTEME_API}/tags", params={"limit": 100})
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"systeme.io respondió {resp.status_code}: {resp.text[:200]}")
        items = resp.json().get("items", [])
    return {
        "configurado_ahora": SYSTEME_TAG_ID,
        "total": len(items),
        "tags": [{"id": t.get("id"), "nombre": t.get("name")} for t in items],
    }


@app.get("/systeme/test")
async def systeme_test(request: Request, email: str = "prueba@ejemplo.com"):
    """Diagnóstico: crea un contacto de prueba y dice exactamente qué falló."""
    user = current_user(request)
    if not user or is_guest(user):
        raise HTTPException(status_code=401, detail="Necesitas entrar con tu cuenta.")
    info = await systeme_sync(email.strip().lower(), "Prueba")
    return {"tag_configurado": SYSTEME_TAG_ID, **info}


@app.get("/historial", response_class=HTMLResponse)
async def ver_historial(request: Request):
    user = current_user(request)
    if not user:
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(historial_page(user))


@app.get("/historial/{id_analisis}", response_class=HTMLResponse)
async def ver_analisis(id_analisis: str, request: Request):
    user = current_user(request)
    if not user:
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    fila = historial_abrir(user, id_analisis, admin=not is_guest(user))
    if not fila:
        raise HTTPException(status_code=404, detail="Ese análisis no existe o no es tuyo.")
    return HTMLResponse(historial_detalle_page(user, fila))


@app.delete("/api/historial/{id_analisis}")
async def borrar_analisis(id_analisis: str, user: str = Depends(require_user)):
    historial_borrar(user, id_analisis)
    return {"ok": True}


@app.get("/leads", response_class=HTMLResponse)
async def ver_leads(request: Request):
    user = current_user(request)
    if not user or is_guest(user):
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(leads_page(user))


class NovedadRequest(BaseModel):
    titulo: str = Field(..., min_length=3, max_length=150)
    texto: str = Field(..., min_length=3, max_length=4000)


@app.get("/novedades", response_class=HTMLResponse)
async def ver_novedades(request: Request):
    user = current_user(request)
    if not user:
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(novedades_page(user))


@app.post("/api/novedades")
async def crear_novedad(req: NovedadRequest, user: str = Depends(require_user)):
    if is_guest(user):
        raise HTTPException(status_code=403, detail="Solo las cuentas pueden publicar novedades.")
    nid = novedades_crear(req.titulo.strip(), req.texto.strip())
    ULTIMA_NOVEDAD["revisado"] = 0.0      # fuerza refresco del aviso
    if not nid:
        raise HTTPException(status_code=502, detail="No se pudo guardar la novedad.")
    return {"ok": True, "id": nid}


@app.delete("/api/novedades/{id_novedad}")
async def borrar_novedad(id_novedad: str, user: str = Depends(require_user)):
    if is_guest(user):
        raise HTTPException(status_code=403, detail="Solo las cuentas pueden borrar novedades.")
    novedades_borrar(id_novedad)
    ULTIMA_NOVEDAD["revisado"] = 0.0
    return {"ok": True}


@app.get("/nichos", response_class=HTMLResponse)
async def ver_nichos(request: Request):
    user = current_user(request)
    if not user or is_guest(user):
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(nichos_page(user))


@app.get("/actividad", response_class=HTMLResponse)
async def ver_actividad(request: Request):
    user = current_user(request)
    if not user or is_guest(user):
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(actividad_page(user))


@app.get("/actividad.csv")
async def actividad_csv(request: Request):
    user = current_user(request)
    if not user or is_guest(user):
        raise HTTPException(status_code=401, detail="Necesitas entrar con tu cuenta.")
    nombres = {l.get("email"): l.get("nombre") for l in _read_leads().values()}
    filas = ["usuario,nombre,tipo,agente,titulo,fuente,comentarios,fecha"]
    for f in actividad_listar(500):
        u = f.get("usuario", "")
        invitado = u.startswith(GUEST_PREFIX)
        correo = u[len(GUEST_PREFIX):] if invitado else u
        titulo = (f.get("titulo") or "").replace('"', "'")
        filas.append(f'"{correo}","{nombres.get(correo, "")}","{"invitado" if invitado else "cuenta"}",'
                     f'"{f.get("agente","")}","{titulo}","{f.get("fuente","")}",'
                     f'{f.get("total_comentarios", 0)},"{f.get("creado","")}"')
    return Response("\n".join(filas), media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=actividad.csv"})


class CreditosRequest(BaseModel):
    email: str
    creditos: Optional[int] = None
    ilimitado: Optional[bool] = None


@app.post("/api/creditos")
async def api_creditos(req: CreditosRequest, user: str = Depends(require_user)):
    if is_guest(user):
        raise HTTPException(status_code=403, detail="Solo las cuentas pueden cargar créditos.")
    email = req.email.strip().lower()
    if req.ilimitado is not None:
        await marcar_ilimitado(email, req.ilimitado)
    if req.creditos:
        await cargar_creditos(email, int(req.creditos))
    return {"ok": True, "creditos": creditos_de(email)}


@app.post("/api/usuarios/{email}/clave")
async def api_reset_clave(email: str, user: str = Depends(require_user)):
    if is_guest(user):
        raise HTTPException(status_code=403, detail="Solo las cuentas pueden restablecer contraseñas.")
    nueva = await clave_temporal(email.strip().lower())
    if not nueva:
        raise HTTPException(status_code=404, detail="No encontré ese usuario.")
    return {"ok": True, "clave": nueva}


@app.delete("/api/usuarios/{email}")
async def api_borrar_usuario(email: str, user: str = Depends(require_user)):
    if is_guest(user):
        raise HTTPException(status_code=403, detail="Solo las cuentas pueden eliminar usuarios.")
    resultado = lead_borrar(email.strip().lower())
    logger.info("Usuario eliminado por %s: %s (%s análisis)", user, email, resultado["analisis"])
    return {"ok": True, **resultado}


@app.get("/leads.csv")
async def leads_csv(request: Request):
    user = current_user(request)
    if not user or is_guest(user):
        raise HTTPException(status_code=401, detail="Necesitas entrar con tu cuenta.")
    filas = ["correo,nombre,creado,usos,feedback"]
    for lead in _read_leads().values():
        coment = " | ".join(f"{'+' if f.get('util') else '-'} {f.get('texto','')}" for f in lead.get("feedback", []))
        coment = coment.replace('"', "'")
        filas.append(f'"{lead.get("email","")}","{lead.get("nombre","")}","{lead.get("creado","")}",{lead.get("usos",0)},"{coment}"')
    return Response("\n".join(filas), media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=leads.csv"})


@app.get("/logout")
async def logout():
    response = HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    response.delete_cookie(COOKIE_NAME)
    return response


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = current_user(request)
    if not user:
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(dash_page(user))


@app.get("/cerebro/{slug}", response_class=HTMLResponse)
async def cerebro_view(slug: str, request: Request):
    user = current_user(request)
    if not user:
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    if slug not in CEREBROS:
        raise HTTPException(status_code=404, detail="Ese cerebro no existe.")
    return HTMLResponse(brain_page(slug, user))


@app.post("/paste", response_model=AnalyzeResponse)
async def analyze_pasted(req: PasteRequest, user: str = Depends(require_user)):
    check_quota(user, req.modo)
    comments = parse_pasted(req.texto)
    if len(comments) < 10:
        raise HTTPException(status_code=400, detail="Pega al menos 10 comentarios, uno por línea (lo ideal son 200-300).")
    result = await run_analysis(comments, "comentarios pegados de redes sociales", req.modo, req.nicho, modelo_para(user))
    titulo = (req.nicho or "").strip() or f"{len(comments)} comentarios pegados"
    return await build_response("Comentarios pegados", "texto pegado", comments, req.modo, result, user, titulo=titulo)


@app.post("/copy", response_model=AnalyzeResponse)
async def generar_copy(req: CopyRequest, user: str = Depends(require_user)):
    check_quota_copy(user)
    if req.paquete and is_guest(user):
        raise HTTPException(status_code=403, detail="El paquete de 5 formatos está disponible para cuentas. Elige un solo formato.")

    fila = historial_abrir(user, req.analisis_id)
    if not fila:
        raise HTTPException(status_code=404, detail="No encontré ese análisis en tu historial.")
    datos = fila.get("datos") or {}
    if not datos.get("mercado"):
        raise HTTPException(status_code=400, detail="Copy necesita un informe del Analista de Mercado, no uno del Radar.")

    veredicto = ((datos.get("mercado") or {}).get("veredicto") or {}).get("recomendacion", "").upper()
    if veredicto.startswith("NO"):
        raise HTTPException(
            status_code=400,
            detail="El Analista recomendó NO CREAR este producto. Analiza otro tema con el Radar antes de escribir contenido de venta.",
        )

    resultado = await run_copy(datos, req.formato, req.angulo_a, req.angulo_b,
                               req.estado, req.paquete, modelo_para(user))

    titulo_base = fila.get("titulo") or "Análisis"
    cuantas = len(resultado.piezas)
    etiqueta = "paquete de 5 formatos" if req.paquete else FORMATOS_COPY.get(req.formato, req.formato)
    restantes = await consumir_credito(guest_email(user)) if is_guest(user) else None
    respuesta = AnalyzeResponse(
        fuente=f"Copy · {etiqueta}", consulta=titulo_base,
        total_comentarios=datos.get("total_comentarios", 0),
        modelo=GEMINI_MODEL, modo="copy", copywriting=resultado, restantes=restantes,
    )
    try:
        historial_guardar(usuario=user, agente="copy", titulo=f"Copy · {titulo_base}",
                          fuente=f"{etiqueta} · {cuantas} pieza(s)",
                          total=datos.get("total_comentarios", 0),
                          datos=respuesta.model_dump(mode="json"))
    except Exception:
        logger.exception("No se pudo guardar el copy en el historial")
    return respuesta


@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze_facebook(req: AnalyzeRequest, user: str = Depends(require_user)):
    block_guest(user)
    check_quota(user, req.modo)
    url = str(req.url)
    if "facebook.com" not in url and "fb.watch" not in url:
        raise HTTPException(status_code=400, detail="El enlace debe ser de Facebook.")
    comments = await fetch_facebook_comments(url)
    if not comments:
        raise HTTPException(status_code=404, detail=NO_COMMENTS)
    result = await run_analysis(comments, "una publicación de Facebook", req.modo, req.nicho, modelo_para(user))
    return await build_response("Facebook", url, comments, req.modo, result, user)


@app.post("/youtube/video", response_model=AnalyzeResponse)
async def analyze_youtube_video(req: AnalyzeRequest, user: str = Depends(require_user)):
    check_quota(user, req.modo)
    _require_youtube_key()
    url = str(req.url)
    video_id = extract_video_id(url)
    if not video_id:
        raise HTTPException(status_code=400, detail="No reconozco ese enlace de YouTube.")

    async with httpx.AsyncClient(timeout=30) as client:
        videos = await get_video_details(client, [video_id])
        if not videos:
            raise HTTPException(status_code=404, detail="Ese video no existe o es privado.")
        comments, info = await collect_youtube(client, videos)

    if not comments:
        raise HTTPException(status_code=404, detail="Este video no tiene comentarios o los tiene desactivados.")
    result = await run_analysis(comments, f'un video de YouTube titulado "{videos[0]["titulo"]}"', req.modo, req.nicho, modelo_para(user))
    return await build_response("YouTube", url, comments, req.modo, result, user, info,
                                titulo=videos[0]["titulo"])


@app.post("/youtube/search", response_model=AnalyzeResponse)
async def analyze_youtube_search(req: YouTubeSearchRequest, user: str = Depends(require_user)):
    check_quota(user, req.modo)
    _require_youtube_key()
    query = req.query.strip()

    async with httpx.AsyncClient(timeout=30) as client:
        search = await yt_get(client, "search", {
            "part": "snippet", "q": query, "type": "video",
            "maxResults": 25, "relevanceLanguage": "es", "order": "relevance",
        })
        ids = [it["id"]["videoId"] for it in (search or {}).get("items", []) if it.get("id", {}).get("videoId")]
        if not ids:
            raise HTTPException(status_code=404, detail="No encontré videos para esa búsqueda. Prueba con otras palabras.")

        details = await get_video_details(client, ids)
        best = sorted((v for v in details if v["comment_count"] > 0),
                      key=lambda v: v["comment_count"], reverse=True)[: req.max_videos]
        if not best:
            raise HTTPException(status_code=404, detail="Los videos encontrados no tienen comentarios disponibles.")
        comments, info = await collect_youtube(client, best)

    if not comments:
        raise HTTPException(status_code=404, detail="No pude leer comentarios de los videos encontrados.")
    result = await run_analysis(comments, f'varios videos de YouTube sobre "{query}"', req.modo, req.nicho or query, modelo_para(user))
    return await build_response(f"YouTube · {len(info)} videos", query, comments, req.modo, result, user, info,
                                titulo=query)
