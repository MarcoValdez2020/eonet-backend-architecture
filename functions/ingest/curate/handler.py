"""Lambda `curate`: consume notificaciones S3 (via SQS) de objetos nuevos en
raw/, los transforma (explode + enriquecimiento geografico) y escribe
Parquet particionado a curated/. Ver docs/decisiones.md para el porque de
cada pieza (Polars vs pandas, reverse_geocoder + guard offshore, imagen de
contenedor en vez de zip).

Disparo: S3 ObjectCreated en raw/ -> SQS CurateQueue -> este handler
(batchSize=1: cada objeto raw puede traer miles de eventos por si solo, no
tiene sentido mezclar varios objetos en una invocacion).
"""

import io
import json
import logging
import os
import re
import urllib.parse

import boto3
import geo

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = os.environ["BUCKET_NAME"]
OFFSHORE_THRESHOLD_KM = float(os.environ.get("OFFSHORE_THRESHOLD_KM", "100"))

s3 = boto3.client("s3")


def _deterministic_source_id(raw_key: str) -> str:
    """Deriva un identificador de archivo a partir del key crudo de origen,
    para que un reintento de la MISMA invocacion (redelivery de SQS por
    fallo transitorio) sobreescriba su propio intento anterior en vez de
    acumular part-files duplicados en la misma particion. No resuelve (ni
    pretende resolver) el solape ya conocido entre ventanas diarias de
    `extract` -- eso se difirio a SeenEvents, cuando se construya `alarms`."""
    without_ext = raw_key.rsplit(".", 1)[0]
    return re.sub(r"[^A-Za-z0-9_-]", "-", without_ext.replace("/", "-"))


def _write_curated(groups, source_id: str) -> int:
    written = 0
    for category_id, year, month, group_df in groups:
        buf = io.BytesIO()
        group_df.write_parquet(buf)
        key = f"curated/eonet/category={category_id}/year={year}/month={month:02d}/part-{source_id}.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
        logger.info("Escritas %s filas en s3://%s/%s", group_df.height, BUCKET_NAME, key)
        written += group_df.height
    return written


def _process_s3_record(s3_record: dict) -> None:
    bucket = s3_record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])

    raw_obj = s3.get_object(Bucket=bucket, Key=key)
    raw_body = json.loads(raw_obj["Body"].read())
    events = raw_body.get("events", [])
    window_start = raw_body.get("window_start", "")
    window_end = raw_body.get("window_end", "")

    if not events:
        logger.info("Objeto raw sin eventos, nada que curar: s3://%s/%s", bucket, key)
        return

    df = geo.explode_events(events, window_start, window_end)
    df = geo.enrich(df, OFFSHORE_THRESHOLD_KM)
    groups = geo.partition_groups(df)

    source_id = _deterministic_source_id(key)
    total_rows = _write_curated(groups, source_id)
    logger.info(
        "Curado s3://%s/%s: %s eventos -> %s filas en %s particion(es)",
        bucket, key, len(events), total_rows, len(groups),
    )


def handler(event, context):
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            body = json.loads(record["body"])
        except (KeyError, json.JSONDecodeError) as exc:
            logger.error("Mensaje SQS %s no es JSON valido: %s", message_id, exc)
            batch_item_failures.append({"itemIdentifier": message_id})
            continue

        s3_records = body.get("Records")
        if not s3_records:
            # S3 manda un mensaje de prueba (s3:TestEvent, sin "Records") la
            # primera vez que se configura la notificacion -- no es un error.
            logger.info("Mensaje SQS %s sin Records S3 (ej. s3:TestEvent), se ignora", message_id)
            continue

        try:
            for s3_record in s3_records:
                _process_s3_record(s3_record)
        except Exception:
            logger.exception("Fallo procesando mensaje SQS %s", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
