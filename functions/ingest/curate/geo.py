"""Transformacion pura de eventos EONET: explode + enriquecimiento geografico.

Sin dependencias de AWS aqui a proposito -- handler.py hace el I/O (S3, SQS),
este modulo solo recibe/devuelve estructuras Python/Polars. Facilita probarlo
en un venv normal sin Docker ni credenciales de AWS.
"""

import datetime as dt
import json
import logging
import math
from pathlib import Path

import polars as pl
import reverse_geocoder

logger = logging.getLogger()

DEFAULT_OFFSHORE_THRESHOLD_KM = 100.0

# mode=1 fuerza el geocodificador a single-process. El modo por defecto de la
# libreria (mode=2) usa multiprocessing.Process + memoria compartida en CADA
# query, no solo al construir el indice -- innecesario y fragil dentro de una
# Lambda (microVM Firecracker) para un k-d tree de ~145k ciudades que resuelve
# miles de consultas en milisegundos de todas formas.
#
# Construccion PEREZOSA (no a nivel de modulo): construir el k-d tree tarda
# lo suficiente como para, en un cold start, competir con el limite de 10s de
# la fase INIT de Lambda (que es aparte del timeout de la funcion, y NO se
# puede extender) -- se confirmo esto en un deploy real: la primera
# invocacion de un cold start marco "Status: timeout" en el INIT_REPORT de
# CloudWatch. Al construirlo dentro del handler (fase INVOKE) en vez de al
# importar el modulo (fase INIT), el costo de construccion cae bajo el
# timeout de la funcion (300s, con margen de sobra) en vez del limite fijo
# de INIT. Se cachea en este global para reusarse entre invocaciones de un
# mismo container "warm" (mismo patron que el cliente boto3.client("s3") de
# extract, solo que con inicializacion diferida al primer uso real).
_geocoder_instance = None


def _get_geocoder():
    global _geocoder_instance
    if _geocoder_instance is None:
        _geocoder_instance = reverse_geocoder.RGeocoder(mode=1, verbose=False)
    return _geocoder_instance

# Tabla estatica ISO2 -> {name, continent}, generada una sola vez con
# pycountry/pycountry-convert (no es una dependencia de este Lambda, solo se
# uso para generar el JSON). "continent" es el eje "region" que usara forecast
# mas adelante -- se eligio continente (6 valores) y no subregion ONU (~17)
# para tener mas datos por serie historica.
_COUNTRY_REGIONS: dict = json.loads(
    (Path(__file__).parent / "country_regions.json").read_text()
)

EXPLODE_SCHEMA = {
    "event_id": pl.Utf8,
    "event_title": pl.Utf8,
    "event_description": pl.Utf8,
    "event_link": pl.Utf8,
    "closed": pl.Utf8,
    "is_closed": pl.Boolean,
    "category_id": pl.Utf8,
    "category_title": pl.Utf8,
    "categories_all": pl.Utf8,
    "source_ids": pl.Utf8,
    "source_urls": pl.Utf8,
    "geometry_date": pl.Utf8,
    "geometry_type": pl.Utf8,
    "longitude": pl.Float64,
    "latitude": pl.Float64,
    "coordinates_json": pl.Utf8,
    "magnitude_value": pl.Float64,
    "magnitude_unit": pl.Utf8,
    "window_start": pl.Utf8,
    "window_end": pl.Utf8,
    "ingested_at": pl.Utf8,
}


def polygon_centroid(coordinates) -> tuple[float, float]:
    ring = coordinates[0] if isinstance(coordinates[0][0], list) else coordinates
    lons = [point[0] for point in ring]
    lats = [point[1] for point in ring]
    return sum(lons) / len(lons), sum(lats) / len(lats)


def explode_events(events: list[dict], window_start: str, window_end: str) -> pl.DataFrame:
    """Portado de main.py (pandas) a Polars. Una fila por punto de geometry."""
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    seen = set()

    for event in events:
        categories = event.get("categories") or []
        sources = event.get("sources") or []
        category_id = categories[0]["id"] if categories else None
        category_title = categories[0]["title"] if categories else None
        categories_all = "|".join(c["id"] for c in categories)
        source_ids = "|".join(s["id"] for s in sources)
        source_urls = "|".join(s["url"] for s in sources)

        for geometry in event.get("geometry") or []:
            geometry_type = geometry.get("type")
            coordinates = geometry.get("coordinates")
            if geometry_type == "Point":
                longitude, latitude = coordinates[0], coordinates[1]
            elif geometry_type == "Polygon":
                longitude, latitude = polygon_centroid(coordinates)
            else:
                longitude, latitude = None, None

            geometry_date = geometry.get("date")
            key = (event["id"], geometry_date, longitude, latitude)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "event_id": event["id"],
                "event_title": event.get("title"),
                "event_description": event.get("description"),
                "event_link": event.get("link"),
                "closed": event.get("closed"),
                "is_closed": event.get("closed") is not None,
                "category_id": category_id,
                "category_title": category_title,
                "categories_all": categories_all,
                "source_ids": source_ids,
                "source_urls": source_urls,
                "geometry_date": geometry_date,
                "geometry_type": geometry_type,
                "longitude": longitude,
                "latitude": latitude,
                "coordinates_json": json.dumps(coordinates),
                "magnitude_value": geometry.get("magnitudeValue"),
                "magnitude_unit": geometry.get("magnitudeUnit"),
                "window_start": window_start,
                "window_end": window_end,
                "ingested_at": ingested_at,
            })

    df = pl.DataFrame(rows, schema=EXPLODE_SCHEMA)
    df = df.with_columns(
        pl.col("closed").str.to_datetime(time_zone="UTC", strict=False),
        pl.col("geometry_date").str.to_datetime(time_zone="UTC", strict=False),
    )
    # coord_valid: guard de rango -- ya se detectaron 3 filas reales con
    # coordenadas fuera de rango en el sample (EONET_20162, EONET_20156,
    # EONET_20386). Se quedan en el mismo curated/ (no un rejected/ aparte)
    # con coord_valid=false y sin geocodificar.
    df = df.with_columns(
        (
            pl.col("longitude").is_not_null()
            & pl.col("latitude").is_not_null()
            & pl.col("longitude").is_between(-180, 180)
            & pl.col("latitude").is_between(-90, 90)
        ).alias("coord_valid")
    )
    return df


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_earth_km = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r_earth_km * math.asin(math.sqrt(a))


def enrich(df: pl.DataFrame, offshore_threshold_km: float | None = None) -> pl.DataFrame:
    """Agrega country_iso, country_name, region, admin1, nearest_city, offshore."""
    threshold = offshore_threshold_km or DEFAULT_OFFSHORE_THRESHOLD_KM

    country_iso: list[str | None] = [None] * df.height
    country_name: list[str | None] = [None] * df.height
    region: list[str | None] = [None] * df.height
    admin1: list[str | None] = [None] * df.height
    nearest_city: list[str | None] = [None] * df.height
    offshore: list[bool | None] = [False] * df.height

    valid_idx = [i for i, v in enumerate(df["coord_valid"]) if v]
    if valid_idx:
        # reverse_geocoder es offline (k-d tree sobre dataset GeoNames
        # embebido en el paquete, no se descarga nada en runtime) pero SIEMPRE
        # devuelve la ciudad de tierra mas cercana, incluso para un punto en
        # medio del oceano -- por eso el guard de distancia haversine abajo.
        coords = [(df["latitude"][i], df["longitude"][i]) for i in valid_idx]
        results = _get_geocoder().query(coords)
        for i, (lat, lon), res in zip(valid_idx, coords, results):
            dist_km = _haversine_km(lat, lon, float(res["lat"]), float(res["lon"]))
            if dist_km > threshold:
                offshore[i] = True
                country_iso[i] = "XX"
                lookup = _COUNTRY_REGIONS["XX"]
                country_name[i] = lookup["name"]
                region[i] = lookup["continent"]
                nearest_city[i] = res["name"]
                admin1[i] = res["admin1"]
            else:
                offshore[i] = False
                cc = res["cc"]
                country_iso[i] = cc
                lookup = _COUNTRY_REGIONS.get(cc)
                if lookup:
                    country_name[i] = lookup["name"]
                    region[i] = lookup["continent"]
                else:
                    logger.warning("cc de reverse_geocoder sin match en country_regions.json: %s", cc)
                admin1[i] = res["admin1"]
                nearest_city[i] = res["name"]

    return df.with_columns(
        pl.Series("country_iso", country_iso, dtype=pl.Utf8),
        pl.Series("country_name", country_name, dtype=pl.Utf8),
        pl.Series("region", region, dtype=pl.Utf8),
        pl.Series("admin1", admin1, dtype=pl.Utf8),
        pl.Series("nearest_city", nearest_city, dtype=pl.Utf8),
        pl.Series("offshore", offshore, dtype=pl.Boolean),
    )


def partition_groups(df: pl.DataFrame) -> list[tuple[str, int, int, pl.DataFrame]]:
    """Agrupa por (category_id, year, month) de geometry_date para escribir
    curated/eonet/category=<cat>/year=<yyyy>/month=<mm>/part-*.parquet.
    Particiona por fecha del EVENTO (geometry_date), no de ingesta -- ver
    docs/decisiones.md."""
    if df.height == 0:
        return []

    # Fallback a window_end cuando geometry_date no parseo (deberia ser raro:
    # EONET siempre trae date por punto de geometria, pero es dato externo).
    df = df.with_columns(
        pl.coalesce(
            [pl.col("geometry_date"), pl.col("window_end").str.to_datetime(time_zone="UTC", strict=False)]
        ).alias("_partition_date"),
        pl.col("category_id").fill_null("uncategorized"),
    )
    df = df.with_columns(
        pl.col("_partition_date").dt.year().alias("_year"),
        pl.col("_partition_date").dt.month().alias("_month"),
    )

    groups = []
    for keys, group_df in df.group_by(["category_id", "_year", "_month"]):
        category_id, year, month = keys
        groups.append((category_id, int(year), int(month), group_df.drop(["_partition_date", "_year", "_month"])))
    return groups
