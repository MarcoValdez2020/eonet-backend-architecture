"""Lambda `heatmap`: GET /heatmap -- densidad geografica de eventos EONET,
agregada por celda de un grid (lat/lon redondeados a `cell_size` grados) y
categoria. Pensado para pintar un mapa de calor de riesgo, no una lista de
eventos (eso ya lo cubre GET /events).

A diferencia de /events, aqui NO se deduplica a "una fila por evento": se
usan TODOS los datapoints geometricos de cada evento (una tormenta que se
movio por 5 celdas suma densidad en las 5), pero con COUNT(DISTINCT
event_id) por celda -- asi un evento con varios datapoints DENTRO de la
misma celda solo cuenta 1 vez ahi. Decision tomada explicitamente con el
usuario: para riesgo de infraestructura importa TODO lugar por el que paso
un peligro, no solo su ultima posicion conocida.

Disparo: HTTP API (API Gateway v2), mismo JWT authorizer que /events.
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

DEFAULT_CELL_SIZE = 1.0 # grados (~111km en el ecuador)
MIN_CELL_SIZE = 0.1 # ~11km -- por debajo de esto ya no es un "mapa de calor", es casi el dato crudo
MAX_CELL_SIZE = 10.0 # ~1100km -- por encima de esto deja de ser util como grid


def _parse_filters(params: dict) -> dict:
    filters = parse_common_filters(params)

    cell_size = params.get("cell_size")
    if cell_size:
        try:
            cell_size_f = float(cell_size)
        except ValueError as exc:
            raise ValidationError(f"cell_size invalido: {cell_size}") from exc
        if not (MIN_CELL_SIZE <= cell_size_f <= MAX_CELL_SIZE):
            raise ValidationError(f"cell_size debe estar entre {MIN_CELL_SIZE} y {MAX_CELL_SIZE}")
        filters["cell_size"] = cell_size_f
    else:
        filters["cell_size"] = DEFAULT_CELL_SIZE

    return filters


def _build_query(filters: dict) -> str:
    where_clause = " AND ".join(common_conditions(filters))
    cell_size = filters["cell_size"]

    # Misma expresion repetida en SELECT y GROUP BY (no el alias) para que
    # Presto agrupe por el valor crudo -- redondear solo en el SELECT es
    # cosmetico (evita ruido de precision de punto flotante en la respuesta)
    # y no afecta el agrupamiento porque la misma entrada siempre produce el
    # mismo float.
    cell_lat_expr = f"FLOOR(latitude / {cell_size}) * {cell_size}"
    cell_lon_expr = f"FLOOR(longitude / {cell_size}) * {cell_size}"

    return f"""
            SELECT
                ROUND({cell_lat_expr}, 4) AS cell_lat,
                ROUND({cell_lon_expr}, 4) AS cell_lon,
                category_id,
                COUNT(DISTINCT event_id) AS event_count
            FROM {ATHENA_TABLE}
            WHERE {where_clause}
            GROUP BY {cell_lat_expr}, {cell_lon_expr}, category_id
            ORDER BY event_count DESC
            """.strip()


def _coerce_row(row: dict) -> dict:
    return {
        "cell_lat": float(row["cell_lat"]),
        "cell_lon": float(row["cell_lon"]),
        "category_id": row["category_id"],
        "event_count": int(row["event_count"]),
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

    cells = [_coerce_row(r) for r in rows]
    logger.info("Devueltas %s celdas", len(cells))
    return json_response(200, {"cell_size": filters["cell_size"], "count": len(cells), "cells": cells})
