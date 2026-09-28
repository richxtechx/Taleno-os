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
SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_urlsafe(32)
SESSION_DAYS = 30
COOKIE_NAME = "taleno_session"
GUEST_PREFIX = "invitado:"
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "1") != "0"   # ponlo en 0 solo para probar en local (http)
GUEST_MODE = os.getenv("GUEST_MODE", "0") == "1"         # 1 = permite entrar sin cuenta
GUEST_FULL = os.getenv("GUEST_FULL", "0") == "1"         # 1 = los invitados también pueden usar Facebook (cuesta Apify)
GUEST_LIMIT = int(os.getenv("GUEST_LIMIT", "3"))         # análisis gratis por correo
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


def _sb(metodo: str, params: dict = None, payload=None) -> list:
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/leads"
    try:
        with httpx.Client(timeout=15, headers=_sb_headers()) as client:
            resp = client.request(metodo, url, params=params, json=payload)
        if resp.status_code >= 400:
            logger.warning("Supabase %s -> %s: %s", metodo, resp.status_code, resp.text[:200])
            return []
        return resp.json() if resp.text else []
    except Exception:
        logger.exception("Supabase: error de conexión")
        return []


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
            "usos": 0, "systeme": False, "feedback": []}


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


def check_quota(user: str):
    """Corta al invitado que ya gastó sus pruebas gratis, ANTES de llamar a la IA."""
    if not is_guest(user):
        return
    if lead_usos(guest_email(user)) >= GUEST_LIMIT:
        raise HTTPException(
            status_code=429,
            detail=f"Ya usaste tus {GUEST_LIMIT} análisis de prueba. Escríbeme y te doy acceso completo.",
        )


def modelo_para(user: str) -> Optional[str]:
    return GUEST_MODEL if is_guest(user) else None

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
apify_client = ApifyClientAsync(APIFY_API_TOKEN) if APIFY_API_TOKEN else None


# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------
Modo = Literal["rapido", "mercado"]


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
        "nombre": "Analista de Mercado",
        "modo": "mercado",
        "lema": "Del dolor al producto, en 8 pasos",
        "desc": "Pega aquí los comentarios (o tráelos desde el Radar). Detecta el problema urgente específico, propone el producto con su mecanismo único y da un veredicto: crear o no crear.",
        "tiempo": "1 a 4 min",
        "fuentes": ["paste"],
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
    font: 16px/1.55 "Instrument Sans", system-ui, -apple-system, sans-serif; -webkit-text-size-adjust: 100%; }
  h1, h2, .marca, .rec, .nombre { font-family: "Unbounded", "Instrument Sans", sans-serif; letter-spacing: -0.02em; }
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
  .btn-sesion { font: 600 14px "Instrument Sans", sans-serif; text-decoration: none; padding: 9px 16px;
    border-radius: 10px; border: 1.5px solid var(--linea); color: var(--gris); background: var(--papel); }
  .btn-sesion.primario { background: var(--naranja); border-color: var(--naranja); color: #fff; }
  .chip { font-size: 12px; font-weight: 700; color: var(--naranja); background: #fff1e9;
    border-radius: 999px; padding: 5px 10px; white-space: nowrap; }

  /* ---- barra lateral ---- */
  .lateral { position: fixed; top: var(--barra-h); bottom: 0; left: 0; width: var(--lateral-w); z-index: 20;
    background: var(--papel); border-right: 1px solid var(--linea); padding: 16px 12px;
    display: flex; flex-direction: column; gap: 4px; overflow-y: auto; transition: width .18s, transform .18s; }
  .grupo { font-size: 11px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase;
    color: var(--gris); padding: 14px 12px 6px; }
  .nav { display: flex; align-items: center; gap: 12px; padding: 11px 12px; border-radius: 10px;
    text-decoration: none; color: var(--tinta); font-size: 15px; font-weight: 600; }
  .nav:hover { background: var(--fondo); }
  .nav[aria-current="page"] { background: #fff1e9; color: #b8430f; }
  .nav .ic { width: 22px; text-align: center; font-size: 16px; flex: none; }
  .nav .tx { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .nav.mudo { color: var(--gris); font-weight: 400; cursor: default; }
  .pie-lateral { margin-top: auto; padding: 12px; font-size: 12px; color: var(--gris); }

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
  .tab { padding: 10px 15px; font: 600 14px "Instrument Sans", sans-serif; color: var(--gris);
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
  .submit { width: 100%; margin-top: 16px; padding: 16px 22px; font: 700 16px "Instrument Sans", sans-serif;
    color: #fff; background: var(--naranja); border: 0; border-radius: 12px; cursor: pointer; }
  .submit:disabled { opacity: .6; cursor: wait; }
  button:focus-visible, a:focus-visible { outline: 3px solid rgba(255,107,43,.4); outline-offset: 2px; }
  [hidden] { display: none !important; }
  .status { margin: 16px 0 0; color: var(--gris); min-height: 1.5em; }
  .status.error { color: var(--no); font-weight: 600; }
  .spinner { display: inline-block; width: 14px; height: 14px; margin-right: 8px; vertical-align: -2px;
    border: 2px solid var(--linea); border-top-color: var(--naranja); border-radius: 50%; animation: giro .8s linear infinite; }
  @keyframes giro { to { transform: rotate(360deg); } }
  @media (prefers-reduced-motion: reduce) { .spinner, .lateral, .contenido { animation: none; transition: none; } }
  @media (min-width: 720px) { .submit { width: auto; } }
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
  blockquote { margin: 12px 0 0; padding: 0 0 0 14px; border-left: 2px solid var(--c, var(--azul));
    font: italic 17px/1.5 "Newsreader", Georgia, serif; color: #33445c; }
  blockquote + blockquote { margin-top: 8px; }
  .empty { color: var(--gris); font-style: italic; margin: 0; }
  .verdict { margin-top: 26px; padding: 24px; border-radius: 16px; color: #fff; background: var(--v); }
  .verdict .top { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }
  .verdict .rec { font-size: 24px; font-weight: 700; }
  .verdict .score { font-size: 14px; font-weight: 600; background: rgba(255,255,255,.2); padding: 6px 12px; border-radius: 999px; }
  .verdict p { margin: 12px 0 0; }
  .verdict .next { margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(255,255,255,.3); }
  .problem .macro { text-decoration: line-through; color: var(--gris); }
  .problem .specific { font: italic 20px/1.4 "Newsreader", Georgia, serif; margin: 10px 0 0; }
  .product { border: 2px solid var(--tinta); }
  .product .nombre { font-size: 22px; font-weight: 700; margin: 0; line-height: 1.2; }
  .product .price { font-size: 30px; font-weight: 700; margin: 14px 0 0; color: var(--naranja); }
  .product ul, .item ul { margin: 8px 0 0; padding-left: 20px; }
  .kv { color: var(--gris); font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em; margin: 14px 0 2px; }
  .meter { height: 8px; background: var(--linea); border-radius: 99px; overflow: hidden; margin-top: 8px; }
  .meter span { display: block; height: 100%; background: var(--dolor); }
  .fbbox { margin-top: 36px; background: var(--papel); border: 1px dashed var(--linea); border-radius: 16px; padding: 22px; }
  .fbbox textarea { min-height: 90px; margin-top: 12px; }
  .fbrow { display: flex; gap: 8px; margin-top: 12px; }
  .fbrow .tab[aria-pressed="true"] { background: var(--naranja); border-color: var(--naranja); color: #fff; }
  .extraidos .acciones { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
  .extraidos .tab { cursor: pointer; }
  .extraidos .tab.destacado { background: var(--naranja); border-color: var(--naranja); color: #fff; }
  .crudos { background: var(--papel); border: 1px solid var(--linea); border-radius: 14px; padding: 16px;
    max-height: 320px; overflow: auto; white-space: pre-wrap; word-break: break-word;
    font: 14px/1.5 "Instrument Sans", sans-serif; color: #33445c; margin: 0; }
  .sources a { display: block; background: var(--papel); border: 1px solid var(--linea); border-radius: 14px;
    padding: 14px 18px; margin-bottom: 10px; text-decoration: none; }
  .sources small { color: var(--gris); display: block; }
"""

FONTS = """<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;600;700&family=Unbounded:wght@600;700&family=Newsreader:ital,opsz@1,6..72&display=swap" rel="stylesheet">"""

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
})();
"""


def _nombre_visible(user: str) -> str:
    if is_guest(user):
        email = guest_email(user)
        return (_read_leads().get(email, {}).get("nombre") or email.split("@")[0]).strip()
    return user.split("@")[0]


def topbar(user: Optional[str], con_lateral: bool, mostrar_entrar: bool = True) -> str:
    hamb = '<button class="hamb" id="hamb" aria-label="Mostrar u ocultar el menú">☰</button>' if con_lateral else ""
    if user:
        nombre = _nombre_visible(user)
        inicial = (nombre[:1] or "?").upper()
        etiqueta = ""
        if is_guest(user):
            restantes = max(GUEST_LIMIT - lead_usos(guest_email(user)), 0)
            etiqueta = f'<span class="chip">{restantes} de {GUEST_LIMIT}</span>'
        derecha = f"""<div class="usuario">{etiqueta}
          <span class="avatar">{inicial}</span>
          <span class="nombre-usuario">{nombre}</span>
          <a class="btn-sesion" href="/logout">Salir</a></div>"""
    elif mostrar_entrar:
        derecha = '<a class="btn-sesion primario" href="/login">Entrar</a>'
    else:
        derecha = ""
    return f"""<header class="topbar">{hamb}
      <a class="marca" href="/">TΛLENO <b>OS</b></a>
      <span class="sp"></span>{derecha}</header>"""


def sidebar(user: str, activo: str = "") -> str:
    items = ['<a class="nav" href="/" %s><span class="ic">▦</span><span class="tx">Panel</span></a>'
             % ('aria-current="page"' if activo == "panel" else "")]
    items.append('<div class="grupo">Agentes</div>')
    iconos = {"radar": "◎", "analista": "⚑"}
    for slug, c in CEREBROS.items():
        actual = 'aria-current="page"' if activo == slug else ""
        items.append(f'<a class="nav" href="/cerebro/{slug}" {actual}>'
                     f'<span class="ic">{iconos.get(slug, "✦")}</span>'
                     f'<span class="tx">{c["nombre"]}</span></a>')
    items.append('<div class="nav mudo"><span class="ic">+</span><span class="tx">Próximamente</span></div>')
    if not is_guest(user):
        items.append('<div class="grupo">Gestión</div>')
        actual = 'aria-current="page"' if activo == "leads" else ""
        items.append(f'<a class="nav" href="/leads" {actual}><span class="ic">✉</span>'
                     f'<span class="tx">Invitados</span></a>')
    items.append('<div class="pie-lateral">RichTech · Método TΛLENO</div>')
    return '<aside class="lateral">' + "".join(items) + '</aside><div class="velo" id="velo"></div>'


def page(title: str, contenido: str, user: Optional[str] = None, activo: str = "",
         extra_css: str = "", script: str = "", con_lateral: bool = True,
         mostrar_entrar: bool = True) -> str:
    lateral = sidebar(user, activo) if (con_lateral and user) else ""
    clase = "contenido" if lateral else "contenido solo"
    return f"""<!DOCTYPE html>
<html lang="es"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>{title}</title>
{FONTS}
<style>{BASE_CSS}{extra_css}</style>
</head><body>
{topbar(user, bool(lateral), mostrar_entrar)}
{lateral}
<main class="{clase}">{contenido}</main>
<script>{SHELL_JS}{script}</script>
</body></html>"""

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
const el = (id) => document.getElementById(id);
let src = "__INICIAL__";

function setSource(s) {
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

function conectarBotones() {
  const copiar = el("copiar"), alAnalista = el("alAnalista");
  if (copiar) copiar.addEventListener("click", async () => {
    const texto = comentariosExtraidos.join("\\n");
    try {
      await navigator.clipboard.writeText(texto);
      el("copiaStatus").textContent = `${comentariosExtraidos.length} comentarios copiados al portapapeles.`;
    } catch (err) {
      const ta = document.createElement("textarea");
      ta.value = texto; document.body.appendChild(ta); ta.select();
      document.execCommand("copy"); ta.remove();
      el("copiaStatus").textContent = "Comentarios copiados.";
    }
  });
  if (alAnalista) alAnalista.addEventListener("click", () => {
    try { sessionStorage.setItem("comentarios_radar", comentariosExtraidos.join("\\n")); } catch (e) {}
    window.location.href = "/cerebro/analista";
  });
}

function esc(t) { const d = document.createElement("div"); d.textContent = t ?? ""; return d.innerHTML; }
const quotes = (arr, c) => (arr || []).map(q => `<blockquote style="--c:${c}">“${esc(q)}”</blockquote>`).join("");
const lista = (arr) => (arr && arr.length) ? `<ul>${arr.map(x => `<li>${esc(x)}</li>`).join("")}</ul>` : `<p class="empty">Sin evidencia en los comentarios.</p>`;

function fuentes(data) {
  if (!data.videos || !data.videos.length) return "";
  return `<section class="sources"><h2>Videos analizados</h2>` + data.videos.map(v =>
    `<a href="${esc(v.url)}" target="_blank" rel="noopener">${esc(v.titulo)}<small>${esc(v.canal)} · ${v.comentarios_analizados} comentarios</small></a>`).join("") + `</section>`;
}

function renderRadar(data) {
  const a = data.analisis;
  const CATS = [["dolores", "Dolores", "var(--dolor)"], ["objeciones", "Objeciones", "var(--objecion)"], ["deseos", "Deseos", "var(--deseo)"]];
  let html = `<div class="summary"><p>${esc(a.resumen)}</p><div class="meta">${data.total_comentarios} comentarios · ${esc(data.fuente)}</div></div>`;
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

function renderAnalista(data) {
  const m = data.mercado, v = m.veredicto, p = m.propuesta_producto, pu = m.problema_urgente;
  const d = m.dolor_emocional, pay = m.disposicion_a_pagar, r = m.resultado_deseado, b = m.brecha_oportunidad;
  let html = `<div class="summary"><p>${esc(m.resumen_ejecutivo)}</p><div class="meta">${data.total_comentarios} comentarios · ${esc(data.fuente)}</div></div>`;
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

el("form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const cfg = SOURCES[src];
  el("results").innerHTML = "";
  el("status").className = "status";
  el("status").innerHTML = `<span class="spinner"></span>${cfg.wait}`;
  el("btn").disabled = true; el("btn").textContent = "Analizando…";
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
    el("results").innerHTML = data.mercado ? renderAnalista(data) : renderRadar(data);
    conectarBotones();
    el("results").scrollIntoView({ behavior: "smooth", block: "start" });
    el("fbBox").hidden = false;
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
    el("btn").disabled = false; el("btn").textContent = "Analizar";
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
  .invitado small { display: block; color: var(--gris); margin-top: 10px; }
"""


LOGIN_JS = """
const guest = document.getElementById("guest");
if (guest) guest.addEventListener("submit", async (e) => {
  e.preventDefault();
  const s = document.getElementById("gstatus");
  s.className = "status"; s.innerHTML = '<span class="spinner"></span>Preparando tu prueba…';
  try {
    const res = await fetch("/api/invitado", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nombre: document.getElementById("gnombre").value.trim(), email: document.getElementById("gemail").value.trim() }) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : "No se pudo entrar.");
    window.location.href = "/";
  } catch (err) { s.className = "status error"; s.textContent = err.message; }
});

const form = document.getElementById("login");
form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const s = document.getElementById("status");
  s.className = "status"; s.innerHTML = '<span class="spinner"></span>Entrando…';
  try {
    const res = await fetch("/api/login", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: document.getElementById("email").value.trim(), password: document.getElementById("password").value }) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : "No se pudo entrar.");
    window.location.href = "/";
  } catch (err) { s.className = "status error"; s.textContent = err.message; }
});
"""


def login_page(msg: str = "") -> str:
    aviso = f'<p class="status error">{msg}</p>' if msg else ""
    invitado = (f"""<div class="invitado"><span>¿Primera vez? Prueba gratis</span>
        <form id="guest">
          <div class="campo"><input id="gnombre" required placeholder="Tu nombre" aria-label="Nombre"></div>
          <div class="campo"><input id="gemail" type="email" required placeholder="Tu correo" aria-label="Correo"></div>
          <button class="submit" type="submit">Probar gratis ({GUEST_LIMIT} análisis)</button>
        </form>
        <p id="gstatus" class="status"></p>
        <small>Te aviso por correo cuando agregue agentes nuevos. Nada de spam.</small></div>""" if GUEST_MODE else "")
    contenido = f"""<div class="caja">
      <h1>Entrar</h1>
      <p>Tus agentes de investigación de mercado, en un solo lugar.</p>
      {aviso}
      <form id="login">
        <div class="campo"><input id="email" type="email" required placeholder="Correo" aria-label="Correo"></div>
        <div class="campo"><input id="password" type="password" required placeholder="Contraseña" aria-label="Contraseña"></div>
        <button class="submit" type="submit">Entrar</button>
      </form>
      <p id="status" class="status"></p>
      {invitado}
    </div>"""
    return page("Entrar · TΛLENO OS", contenido, None, "", LOGIN_CSS, LOGIN_JS,
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
          <span class="num">PANEL</span><span class="nombre">Invitados</span>
          <span class="desc">Correos capturados y feedback recibido.</span></a>"""
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
        filas += f"""<tr><td>{l.get('nombre','')}<br><span class="fb">{l.get('email','')}</span><br>
          <span class="fb">{sync}</span></td>
          <td>{l.get('usos',0)} / {GUEST_LIMIT}</td><td>{l.get('creado','')[:10]}</td><td>{fb}</td></tr>"""
    if not filas:
        filas = '<tr><td colspan="4">Todavía no hay invitados.</td></tr>'
    contenido = f"""<h1>Invitados</h1>
      <p class="intro">{len(leads)} correos capturados. El feedback que dejan aparece en la última columna.</p>
      <a class="descarga" href="/leads.csv">↓ Descargar CSV</a>
      <div class="wrap"><table><tr><th>Persona</th><th>Usos</th><th>Desde</th><th>Feedback</th></tr>{filas}</table></div>"""
    return page("Invitados · TΛLENO OS", contenido, user, "leads", LEADS_CSS)


def brain_page(slug: str, user: str) -> str:
    c = CEREBROS[slug]
    etiquetas = {
        "paste": "Pegar comentarios",
        "yt-search": "Buscar en YouTube",
        "yt-video": "Video de YouTube",
        "facebook": "Facebook",
    }
    fuentes = [f for f in c.get("fuentes", ["paste"])
               if not (f == "facebook" and is_guest(user) and not GUEST_FULL)]
    primera = fuentes[0]
    if len(fuentes) > 1:
        botones = "".join(
            f'<button class="tab" data-src="{f}" aria-pressed="{"true" if f == primera else "false"}">{etiquetas[f]}</button>'
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
    script = BRAIN_JS.replace("__MODO__", c["modo"]).replace("__INICIAL__", primera)
    return page(f"{c['nombre']} · TΛLENO OS", contenido, user, slug, RESULT_CSS, script)


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
class LoginRequest(BaseModel):
    email: str
    password: str


async def build_response(fuente, consulta, comments, modo, result, user, videos=None) -> AnalyzeResponse:
    restantes = None
    if is_guest(user):
        restantes = await lead_consumir_uso(guest_email(user))
    return AnalyzeResponse(
        fuente=fuente, consulta=consulta, total_comentarios=len(comments),
        modelo=GEMINI_MODEL, modo=modo, videos=videos or [], restantes=restantes,
        comentarios=comments if modo == "rapido" else [], **result,
    )


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {
        "status": "ok", "modelo": GEMINI_MODEL,
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
    esperado = USERS.get(email)
    if not esperado or not secrets.compare_digest(esperado, req.password):
        raise HTTPException(status_code=401, detail="Correo o contraseña incorrectos.")
    response.set_cookie(
        COOKIE_NAME, signer.dumps(email), max_age=SESSION_DAYS * 86400,
        httponly=True, samesite="lax", secure=COOKIE_SECURE,
    )
    return {"ok": True}


class GuestRequest(BaseModel):
    nombre: str = Field(..., min_length=2, max_length=80)
    email: str = Field(..., max_length=120)


class FeedbackRequest(BaseModel):
    util: Optional[bool] = None
    texto: str = Field("", max_length=1000)


@app.post("/api/invitado")
async def entrar_invitado(req: GuestRequest, response: Response):
    if not GUEST_MODE:
        raise HTTPException(status_code=403, detail="El acceso de invitado está desactivado.")
    email = req.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Escribe un correo válido.")
    if lead_usos(email) >= GUEST_LIMIT:
        raise HTTPException(status_code=429, detail=f"Ese correo ya usó sus {GUEST_LIMIT} análisis de prueba.")
    await lead_guardar(email, req.nombre.strip())
    info = await systeme_sync(email, req.nombre.strip())
    await lead_marcar_systeme(email, info.get("contacto", False))
    response.set_cookie(
        COOKIE_NAME, signer.dumps(GUEST_PREFIX + email), max_age=SESSION_DAYS * 86400,
        httponly=True, samesite="lax", secure=COOKIE_SECURE,
    )
    return {"ok": True}


@app.post("/api/feedback")
async def enviar_feedback(req: FeedbackRequest, user: str = Depends(require_user)):
    if is_guest(user):
        await lead_feedback(guest_email(user), req.util, req.texto.strip())
    else:
        logger.info("Feedback de %s: %s %s", user, req.util, req.texto[:200])
    return {"ok": True}


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


@app.get("/leads", response_class=HTMLResponse)
async def ver_leads(request: Request):
    user = current_user(request)
    if not user or is_guest(user):
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/login">')
    return HTMLResponse(leads_page(user))


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
    check_quota(user)
    comments = parse_pasted(req.texto)
    if len(comments) < 10:
        raise HTTPException(status_code=400, detail="Pega al menos 10 comentarios, uno por línea (lo ideal son 200-300).")
    result = await run_analysis(comments, "comentarios pegados de redes sociales", req.modo, req.nicho, modelo_para(user))
    return await build_response("Comentarios pegados", "texto pegado", comments, req.modo, result, user)


@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze_facebook(req: AnalyzeRequest, user: str = Depends(require_user)):
    block_guest(user)
    check_quota(user)
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
    check_quota(user)
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
    return await build_response("YouTube", url, comments, req.modo, result, user, info)


@app.post("/youtube/search", response_model=AnalyzeResponse)
async def analyze_youtube_search(req: YouTubeSearchRequest, user: str = Depends(require_user)):
    check_quota(user)
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
    return await build_response(f"YouTube · {len(info)} videos", query, comments, req.modo, result, user, info)
