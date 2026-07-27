"""Piezas compartidas entre los endpoints de este modulo (api.py = GET
/events, heatmap.py = GET /heatmap): validacion de filtros geo/fecha/
categoria, cliente de Athena y el ciclo start/poll/paginate. Se separo
aqui cuando se agrego heatmap.py para no duplicar esta logica -- antes,
con un solo endpoint, vivia toda dentro de api.py.
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

# Tope duro de espera por la consulta de Athena, con margen bajo el
# limite FIJO de 29s de API Gateway (no configurable, a diferencia del
# timeout de Lambda) -- ver "timeout: 25" en serverless.yml para estas
# funciones, que ya deja margen de sobra sobre este valor.
MAX_ATHENA_WAIT_SECONDS = 20
ATHENA_POLL_INTERVAL_SECONDS = 0.5

athena = boto3.client("athena")


class ValidationError(Exception):
    pass


def parse_common_filters(params: dict) -> dict:
    """Valida y normaliza los filtros que comparten /events y /heatmap
    (categoria, pais, rango de fechas, bbox) -- cada valor que sale de aqui
    ya es seguro para interpolar directo en el SQL (whitelist estricta, no
    hay API de parametros/bind en boto3 para Athena). Cada endpoint agrega
    encima sus propios filtros especificos (limit en /events, cell_size en
    /heatmap)."""
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

    return filters


def common_conditions(filters: dict) -> list[str]:
    """WHERE conditions comunes, listas para unir con AND -- cada endpoint
    agrega las suyas (ej. rn = 1 en /events) antes de armar el WHERE final."""
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
    return conditions


def run_query(sql: str) -> list[dict]:
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


def json_response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
