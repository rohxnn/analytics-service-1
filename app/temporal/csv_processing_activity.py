import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from temporalio import activity
from confluent_kafka import Producer, KafkaException

from app.config import settings
from app.database.db import db
from app.database import operations as csv_upload_repo
from app.api.services.uploads import load_csv, rows_to_json, split_csv
from app.api.validators.uploads import validate_columns
from app.services.gcp_storage import fetch_csv
from app.services.ingestion_validation import validate_ingestion_schema

logger = logging.getLogger("analytics_service.temporal.csv_processing_activity")

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        })
    return _producer


def _push_rows_sync(payloads: List[Any]) -> None:
    """
    Runs in a worker thread (via asyncio.to_thread) — produce()/flush() are
    blocking calls. Flushes once for the whole batch rather than per row (a
    per-row flush forces a network round trip per row, far too slow for large
    CSVs), mirroring app/kafka/consumer.py's DLQ producer pattern.
    """
    producer = _get_producer()
    delivery_error = {}

    def _on_delivery(err, _msg):
        if err is not None:
            delivery_error["error"] = err

    for payload, key in payloads:
        producer.produce(
            settings.KAFKA_TOPIC_INGESTION,
            value=payload.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            callback=_on_delivery,
        )
        producer.poll(0)
        if "error" in delivery_error:
            raise KafkaException(delivery_error["error"])

    remaining = producer.flush(10)
    if remaining > 0:
        raise TimeoutError(f"Timed out waiting for Kafka delivery ({remaining} still in-flight)")
    if "error" in delivery_error:
        raise KafkaException(delivery_error["error"])


@activity.defn
async def csv_fetch_and_validate_activity(record_id: int) -> bool:
    """
    Temporal activity to fetch the CSV file from cloud storage and update status.
    """
    record = await csv_upload_repo.get_record(record_id)
    if not record:
        raise ValueError(f"Record {record_id} not found in database.")

    cloud_storage_path = record["cloud_storage_path"]

    try:
        # Fetch from GCS + parse — both blocking, offloaded from the event loop.
        csv_file = await asyncio.to_thread(fetch_csv, cloud_storage_path)
        df = await asyncio.to_thread(load_csv, csv_file)
    except Exception as exc:
        logger.exception("Failed to fetch/load CSV for record %s", record_id)
        error_meta = {
            "stage": "CSV Fetching",
            "error": "Failed to fetch/load CSV from GCS",
            "exception": str(exc),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        return False

    is_valid, errors = await asyncio.to_thread(validate_columns, df, record["report_type"])
    if not is_valid:
        logger.warning("Validation failed for record %s: %s", record_id, errors)
        error_meta = {
            "stage": "CSV Column Validation",
            "error": "Invalid CSV schema",
            "validation_errors": errors,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        return False

    await csv_upload_repo.update_status(record_id, "in_progress")
    return True


@activity.defn
async def csv_push_to_kafka_activity(record_id: int) -> Dict[str, Any]:
    """
    Temporal activity to process the CSV file row by row and publish
    individual messages to Kafka. Returns {"rows_pushed": int,
    "schema_validation_errors": list} — rows failing pre-publish schema
    validation are skipped (not published) and reported here instead.
    """
    record = await csv_upload_repo.get_record(record_id)
    if not record:
        raise ValueError(f"Record {record_id} not found in database.")

    report_type = record["report_type"]
    cloud_storage_path = record["cloud_storage_path"]

    # Load CSV — blocking I/O + parse, offloaded from the event loop.
    csv_file = await asyncio.to_thread(fetch_csv, cloud_storage_path)
    df = await asyncio.to_thread(load_csv, csv_file)

    is_valid, errors = await asyncio.to_thread(validate_columns, df, report_type)
    if not is_valid:
        logger.warning("Kafka push blocked for record %s due to invalid columns: %s", record_id, errors)
        error_meta = {
            "stage": "CSV Column Validation",
            "error": "Invalid CSV schema - aborting Kafka push",
            "validation_errors": errors,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        raise ValueError("CSV validation failed for Kafka push")

    # Fetch programs / leader categories info from Postgres once for context mapping
    program_info = None
    leader_info = None
    # Use tenant_code from the upload payload (stored in meta_data) as primary source,
    # falling back to DB lookup and then to "mitra" as last resort.
    record_meta = record.get("meta_data") or {}
    if isinstance(record_meta, str):
        try:
            record_meta = json.loads(record_meta)
        except json.JSONDecodeError:
            record_meta = {}

    if not isinstance(record_meta, dict):
        record_meta = {}

    tenant_code = record_meta.get("tenant_code") or "mitra"

    try:
        async with db.pool.acquire() as conn:
            leader_row = await conn.fetchrow(
                "SELECT id, name, description, tenant_code FROM leader_category WHERE name = $1 LIMIT 1",
                record.get("leader_category")
            )
            if leader_row:
                leader_info = {
                    "id": str(leader_row["id"]),
                    "name": leader_row["name"],
                    "description": leader_row["description"],
                }
                tenant_code = leader_row["tenant_code"]

            if leader_row:
                program_row = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code, leaders_id FROM programs WHERE name = $1 AND leaders_id = $2 LIMIT 1",
                    record.get("program_name"), leader_row["id"]
                )
            else:
                program_row = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code, leaders_id FROM programs WHERE name = $1 LIMIT 1",
                    record.get("program_name")
                )

            if program_row:
                program_info = {
                    "id": str(program_row["id"]),
                    "name": program_row["name"],
                    "description": program_row["description"],
                }
                tenant_code = program_row.get("tenant_code", tenant_code)

            if program_row and not leader_info:
                leader_row_from_program = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code FROM leader_category WHERE id = $1 LIMIT 1",
                    program_row["leaders_id"]
                )
                if leader_row_from_program:
                    leader_info = {
                        "id": str(leader_row_from_program["id"]),
                        "name": leader_row_from_program["name"],
                        "description": leader_row_from_program["description"],
                    }
                    tenant_code = leader_row_from_program.get("tenant_code", tenant_code)
    except Exception as db_exc:
        logger.warning("Failed to query program/leader category metadata from DB: %s", db_exc)

    # Fallbacks if DB query returned nothing
    if not leader_info:
        leader_info = {
            "id": str(uuid.uuid4()),
            "name": record.get("leader_category") or "District Leader",
            "description": f"Leader category: {record.get('leader_category') or 'District Leader'}",
        }
    if not program_info:
        program_info = {
            "id": str(uuid.uuid4()),
            "name": record.get("program_name") or "My Program",
            "description": f"Program: {record.get('program_name') or 'My Program'}",
        }

    metadata = {
        "programInfo": program_info,
        "LeaderCategoryInfo": leader_info,
        "tenantCode": tenant_code,
    }

    chunks = split_csv(df)
    payloads = []
    schema_errors = []
    row_number = 0

    for chunk in chunks:
        for payload_str in rows_to_json(chunk, report_type, metadata=metadata):
            row_number += 1
            try:
                payload_dict = json.loads(payload_str)
            except json.JSONDecodeError as exc:
                schema_errors.append({"row": row_number, "problems": [f"Failed to parse generated payload: {exc}"]})
                continue

            # Double-check the generated event against the exact same schema
            # app/kafka/consumer.py enforces at ingestion — catches a row missing
            # a required field (e.g. no Session ID, now that it's no longer
            # auto-generated) here, before it's ever published, rather than
            # relying on the consumer to silently DLQ it later.
            problems = validate_ingestion_schema(payload_dict, report_type, "create")
            if problems:
                schema_errors.append({
                    "row": row_number,
                    "submissionId": payload_dict.get("submissionId"),
                    "sessionId": payload_dict.get("sessionId"),
                    "problems": problems,
                })
                continue

            payloads.append((payload_str, f"{record_id}-{len(payloads)}"))

    if schema_errors:
        logger.warning(
            "record %s: %d of %d row(s) failed pre-publish schema validation and were skipped: %s",
            record_id, len(schema_errors), row_number, schema_errors,
        )

    if payloads:
        try:
            await asyncio.to_thread(_push_rows_sync, payloads)
        except Exception as exc:
            logger.exception("Kafka push failed for record %s", record_id)
            error_meta = {
                "stage": "Kafka Publishing",
                "error": "Failed to publish record",
                "exception": str(exc),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
            raise

    return {"rows_pushed": len(payloads), "schema_validation_errors": schema_errors}


@activity.defn
async def csv_update_status_activity(params: Dict[str, Any]) -> None:
    """
    Temporal activity to update the overall processing status of a csv_upload in PostgreSQL.
    """
    record_id = params["record_id"]
    status = params["status"]
    meta_data = params.get("meta_data")
    await csv_upload_repo.update_status(record_id, status, meta_data)


@activity.defn
async def fetch_pending_csv_uploads_activity() -> List[int]:
    """
    Temporal activity to fetch the IDs of all pending csv_upload records.
    """
    records = await csv_upload_repo.list_by_status("pending")
    return [r["id"] for r in records]
