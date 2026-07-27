"""Lambda `events`: GET /events -- lista de eventos EONET curados, con
filtros opcionales (categoria, pais, rango de fechas, bbox), consultando
Athena sobre la tabla `curated_events` (ver infra/glue.yml).

Una fila por EVENTO (no por datapoint/geometria): un mismo event_id puede
tener muchas filas en curated/ -- aqui se deduplica quedandose con la
geometria mas reciente de cada evento (ROW_NUMBER() OVER PARTITION BY
event_id ORDER BY geometry_date DESC).

Disparo: HTTP API (API Gateway v2), detras del JWT authorizer de Cognito
(provider.httpApi.authorizers en serverless.yml) -- solo llega aqui una
invocacion si el token ya fue validado por API Gateway.
"""

import datetime as dt
import json
import logging
import os
import re
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
ATHENA_TABLE = os.environ["ATHENA_TABLE"]

# Las mismas 13 categorias oficiales de EONET que en infra/glue.yml
# (projection.category.values) -- duplicado a proposito, mismo criterio
# que la logica de fechas de generateRanges
VALID_CATEGORIES = {
    "drought", "dustHaze", "earthquakes", "floods", "landslides", "manmade",
    "seaLakeIce", "severeStorms", "snow", "tempExtremes", "volcanoes",
    "waterColor", "wildfires",
}

COUNTRY_ISO_RE = re.compile(r"^[A-Za-z]{2}$") # ISO2, ej. MX, US, AR, etc.
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$") # YYYY-MM-DD

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000

# Tope duro de espera por la consulta de Athena, con margen bajo el
# limite FIJO de 29s de API Gateway (no configurable, a diferencia del
# timeout de Lambda) -- ver "timeout: 25" en serverless.yml para esta
# funcion, que ya deja margen de sobra sobre este valor.
MAX_ATHENA_WAIT_SECONDS = 20
ATHENA_POLL_INTERVAL_SECONDS = 0.5

athena = boto3.client("athena")

RESULT_COLUMNS = [
    "event_id", "event_title", "event_description", "event_link",
    "category_id", "category_title", "geometry_date", "geometry_type",
    "longitude", "latitude", "magnitude_value", "magnitude_unit",
    "closed", "is_closed", "country_iso", "country_name", "region",
    "admin1", "nearest_city", "offshore",
]


class ValidationError(Exception):
    pass


def _parse_filters(params: dict) -> dict:
    """Valida y normaliza los query params -- cada valor que sale de aqui
    ya es seguro para interpolar directo en el SQL (whitelist estricta,
    no hay API de parametros/bind en boto3 para Athena)."""
    filters = {}

    category = params.get("category")
    if category:
        if category not in VALID_CATEGORIES:
            raise ValidationError(f"category invalida: {category}")
        filters["category"] = category

    country = params.get("country")
    if country:
        if not COUNTRY_ISO_RE.match(country):
            raise ValidationError(f"country invalido (se espera ISO2, ej. MX): {country}")
        filters["country"] = country.upper()

    date_from = params.get("from")
    date_to = params.get("to")
    for label, value in (("from", date_from), ("to", date_to)):
        if value and not DATE_RE.match(value):
            raise ValidationError(f"{label} invalido (se espera YYYY-MM-DD): {value}")
        if value:
            try:
                dt.date.fromisoformat(value)
            except ValueError as exc:
                raise ValidationError(f"{label} invalido: {value}") from exc
    if date_from:
        filters["date_from"] = date_from
    if date_to:
        filters["date_to"] = date_to

    bbox = params.get("bbox")
    if bbox:
        parts = bbox.split(",")
        if len(parts) != 4:
            raise ValidationError("bbox invalido (se esperan 4 valores: min_lon,min_lat,max_lon,max_lat)")
        try:
            min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
        except ValueError as exc:
            raise ValidationError(f"bbox invalido: {bbox}") from exc
        if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180 and -90 <= min_lat <= 90 and -90 <= max_lat <= 90):
            raise ValidationError(f"bbox fuera de rango: {bbox}")
        filters["bbox"] = (min_lon, min_lat, max_lon, max_lat)

    limit = params.get("limit")
    if limit:
        try:
            limit_int = int(limit)
        except ValueError as exc:
            raise ValidationError(f"limit invalido: {limit}") from exc
        if not (1 <= limit_int <= MAX_LIMIT):
            raise ValidationError(f"limit debe estar entre 1 y {MAX_LIMIT}")
        filters["limit"] = limit_int
    else:
        filters["limit"] = DEFAULT_LIMIT

    return filters


def _build_query(filters: dict) -> str:
    conditions = ["coord_valid"]
    if "category" in filters:
        conditions.append(f"category_id = '{filters['category']}'")
    if "country" in filters:
        conditions.append(f"country_iso = '{filters['country']}'")
    if "date_from" in filters:
        conditions.append(f"geometry_date >= TIMESTAMP '{filters['date_from']} 00:00:00'")
    if "date_to" in filters:
        conditions.append(f"geometry_date < TIMESTAMP '{filters['date_to']} 00:00:00' + INTERVAL '1' DAY")
    if "bbox" in filters:
        min_lon, min_lat, max_lon, max_lat = filters["bbox"]
        conditions.append(
            f"longitude BETWEEN {min_lon} AND {max_lon} AND latitude BETWEEN {min_lat} AND {max_lat}"
        )

    where_clause = " AND ".join(conditions)
    columns = ", ".join(RESULT_COLUMNS)

    return f"""
            WITH ranked AS (
                SELECT {columns},
                    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY geometry_date DESC) AS rn
                FROM {ATHENA_TABLE}
                WHERE {where_clause}
            )
            SELECT {columns}
            FROM ranked
            WHERE rn = 1
            ORDER BY geometry_date DESC
            LIMIT {filters['limit']}
            """.strip()


def _run_query(sql: str) -> list[dict]:
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
    )["QueryExecutionId"]

    waited = 0.0
    while waited < MAX_ATHENA_WAIT_SECONDS:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = execution["Status"].get("StateChangeReason", "sin detalle")
            raise RuntimeError(f"Consulta Athena {state}: {reason}")
        time.sleep(ATHENA_POLL_INTERVAL_SECONDS)
        waited += ATHENA_POLL_INTERVAL_SECONDS
    else:
        athena.stop_query_execution(QueryExecutionId=query_id)
        raise TimeoutError(f"Consulta Athena no termino en {MAX_ATHENA_WAIT_SECONDS}s, cancelada")

    rows = []
    header = None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))

    return rows


def _coerce_row(row: dict) -> dict:
    def to_float(v):
        return float(v) if v is not None else None

    def to_bool(v):
        return v == "true" if v is not None else None

    return {
        **row,
        "longitude": to_float(row.get("longitude")),
        "latitude": to_float(row.get("latitude")),
        "magnitude_value": to_float(row.get("magnitude_value")),
        "is_closed": to_bool(row.get("is_closed")),
        "offshore": to_bool(row.get("offshore")),
    }


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    params = event.get("queryStringParameters") or {}

    try:
        filters = _parse_filters(params)
    except ValidationError as exc:
        logger.info("Filtros invalidos: %s", exc)
        return _response(400, {"error": str(exc)})

    sql = _build_query(filters)
    logger.info("Consulta Athena: %s", sql.replace("\n", " "))

    try:
        rows = _run_query(sql)
    except TimeoutError as exc:
        logger.error("Timeout esperando Athena: %s", exc)
        return _response(504, {"error": "La consulta tardo demasiado, intenta con filtros mas especificos"})
    except RuntimeError as exc:
        logger.error("Fallo la consulta Athena: %s", exc)
        return _response(502, {"error": "Fallo la consulta a Athena"})

    events = [_coerce_row(r) for r in rows]
    logger.info("Devueltos %s eventos", len(events))
    return _response(200, {"count": len(events), "events": events})
