"""Lambda `generateRanges`: primer paso del Step Functions de backfill
(stepfunctions/backfill.asl.json). Calcula la lista de rangos mensuales
(start/end) que cubren los ultimos N anios hasta hoy -- el Map state del
state machine hace fan-out sobre esta lista, invocando `extract` una vez
por rango.

No toca S3 ni llama a EONET -- es aritmetica de fechas pura, por eso no
comparte Role con extract/curate (solo necesita poder escribir logs).
"""

import datetime as dt
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BACKFILL_YEARS = 5


def handler(event, context):
    # `end` exclusivo (mismo criterio que _default_window en extract):
    # "manana" para que el ultimo dia de hoy quede incluido. UTC explicito
    # (no dt.date.today(), que toma la hora local del sistema donde corra):
    # Lambda ya corre en UTC, pero esto evita que un test local en otra
    # timezone calcule un rango distinto al que correria en AWS.
    end = dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=1)

    # Ancla al dia 1 del mes de hace BACKFILL_YEARS anios -- backfill siempre
    # trae meses completos, a diferencia de extract (que puede recibir una
    # ventana parcial arbitraria), asi que no hace falta la generalidad de
    # month_windows() en extract/handler.py para fechas de inicio a mitad de
    # mes; ver docs/decisiones.md para el porque de esta pequenia duplicacion.
    start = dt.date(end.year - BACKFILL_YEARS, end.month, 1)

    windows = []
    cur = start
    while cur < end:
        year, month = cur.year, cur.month
        nxt = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
        windows.append({"start": cur.isoformat(), "end": min(nxt, end).isoformat()})
        cur = nxt

    logger.info(
        "Generados %s rangos mensuales para el backfill (%s -> %s)",
        len(windows),
        start.isoformat(),
        end.isoformat(),
    )
    return windows
