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

import logging

from common import (
    ATHENA_TABLE,
    ValidationError,
    common_conditions,
    json_response,
    parse_common_filters,
    run_query,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000

RESULT_COLUMNS = [
    "event_id", "event_title", "event_description", "event_link",
    "category_id", "category_title", "geometry_date", "geometry_type",
    "longitude", "latitude", "magnitude_value", "magnitude_unit",
    "closed", "is_closed", "country_iso", "country_name", "region",
    "admin1", "nearest_city", "offshore",
]


def _parse_filters(params: dict) -> dict:
    filters = parse_common_filters(params)

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
    where_clause = " AND ".join(common_conditions(filters))
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


def handler(event, context):
    params = event.get("queryStringParameters") or {}

    try:
        filters = _parse_filters(params)
    except ValidationError as exc:
        logger.info("Filtros invalidos: %s", exc)
        return json_response(400, {"error": str(exc)})

    sql = _build_query(filters)
    logger.info("Consulta Athena: %s", sql.replace("\n", " "))

    try:
        rows = run_query(sql)
    except TimeoutError as exc:
        logger.error("Timeout esperando Athena: %s", exc)
        return json_response(504, {"error": "La consulta tardo demasiado, intenta con filtros mas especificos"})
    except RuntimeError as exc:
        logger.error("Fallo la consulta Athena: %s", exc)
        return json_response(502, {"error": "Fallo la consulta a Athena"})

    events = [_coerce_row(r) for r in rows]
    logger.info("Devueltos %s eventos", len(events))
    return json_response(200, {"count": len(events), "events": events})
