from typing import Any, Dict, List, Optional, Union
import asyncio
import io
import json
import logging
from datetime import datetime
import pandas as pd
from temporalio.client import Client

from app.config import settings
from app.api.validators.uploads import validate_columns
from app.services.gcp_storage import upload_csv
from app.database import operations
from app.api.exceptions import (
    DuplicateFile,
    InvalidCsvColumns,
    RecordNotFound,
    RecordAlreadyProcessing,
    RecordNotPending,
)

logger = logging.getLogger("analytics_service.api.services.uploads")


# ---------------------------------------------------------------------------
# CSV Processing & Formatting Helpers
# ---------------------------------------------------------------------------

def load_csv(csv_file: Union[io.BytesIO, bytes]) -> pd.DataFrame:
    """Parse an in-memory CSV (BytesIO or raw bytes from GCS) into a DataFrame."""
    if isinstance(csv_file, bytes):
        csv_file = io.BytesIO(csv_file)
    else:
        csv_file.seek(0)
    return pd.read_csv(csv_file)


def split_csv(df: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Split a DataFrame into chunks for processing.
    """
    return [df]


def get_csv_value(row_dict: Dict[str, Any], expected_cols: List[str], target_col: str, default: Any = None) -> Any:
    """
    Finds target_col in expected_cols case-insensitively,
    and returns the corresponding value from row_dict case-insensitively.
    Falls back to alias matching for common CSV column name variations.
    """
    _ALIASES = {
        "district": ["matched_district"],
        "organization": ["detected_organization"],
        "transcript link": ["transcript_link"],
        "image urls": ["image_urls"],
        "pdf urls": ["pdf_urls"],
        "session id": ["session"],
        "user location": ["user_location"],
        "report created at": ["created_at"],
        "date of discussion": ["date_of_discussion"],
    }

    target_lower = target_col.lower()
    matched_col = None
    for col in expected_cols:
        if col.lower() == target_lower:
            matched_col = col
            break

    if not matched_col:
        matched_col = target_col

    actual_lower = matched_col.lower()
    for k, v in row_dict.items():
        if k.lower() == actual_lower:
            return v

    aliases = _ALIASES.get(target_lower, [])
    for alias in aliases:
        for k, v in row_dict.items():
            if k.lower() == alias:
                return v

    return default


def parse_csv_list(val) -> List[str]:
    if pd.isna(val) or val is None or not str(val).strip():
        return []
    s = str(val).strip()
    if (s.startswith("[") and s.endswith("]")) or \
       (s.startswith("(") and s.endswith(")")) or \
       (s.startswith("{") and s.endswith("}")):
        s = s[1:-1].strip()

    if "|" in s:
        raw_items = s.split("|")
    else:
        raw_items = s.split(",")

    cleaned = []
    for x in raw_items:
        x_clean = x.strip().strip("'\"").strip()
        if x_clean:
            cleaned.append(x_clean)
    return cleaned


def get_url_field(val):
    urls = parse_csv_list(val)
    if not urls:
        return None
    if len(urls) == 1:
        return urls[0]
    return urls


def clean_segment(s: str) -> str:
    import re
    s = s.strip()
    pattern_num = r'^\s*\d+[\.\)]\s*'
    s = re.sub(pattern_num, '', s).strip()
    pattern_bullet = r'^\s*[\-\*•]\s*'
    s = re.sub(pattern_bullet, '', s).strip()
    return s


def parse_segments(val, delimiter="|") -> List[str]:
    import re
    if pd.isna(val) or val is None or not str(val).strip():
        return []
    s = str(val).strip()
    if delimiter in s:
        raw_segments = s.split(delimiter)
    elif "\n" in s:
        raw_segments = s.split("\n")
    else:
        numbered_pattern = r'(?:^|\s)\d+(?:\.(?!\d)|\))\s*\S'
        has_numbering = len(re.findall(numbered_pattern, s)) >= 2
        if has_numbering:
            raw_segments = re.split(r'(?:^|\s+)\d+(?:\.(?!\d)|\))\s*', s)
        else:
            raw_segments = [s]

    segments = []
    for x in raw_segments:
        x_clean = clean_segment(x)
        if x_clean:
            segments.append(x_clean)
    return segments


def format_datetime(val, with_ms=True) -> str:
    if pd.isna(val) or val is None:
        val = datetime.utcnow()
    if isinstance(val, str):
        try:
            val = pd.to_datetime(val)
        except Exception:
            return val
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime()
    if isinstance(val, datetime):
        if with_ms:
            return val.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(val)


def _is_row_complete(row_dict: Dict[str, Any], report_type: str) -> Any:
    return True, []


def row_to_json(
    row: pd.Series,
    report_type: str,
    event_type: str = "create",
    metadata: Optional[dict] = None,
) -> str:
    row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

    normalized_type = report_type.lower().strip()
    raw_cols = settings.STORY_CSV_COLUMN if normalized_type == "story" else settings.DISCUSSION_CSV_COLUMN
    try:
        expected_cols = json.loads(raw_cols)
    except Exception:
        expected_cols = []

    try:
        submission_id = int(get_csv_value(row_dict, expected_cols, "id"))
    except Exception:
        submission_id = get_csv_value(row_dict, expected_cols, "id")

    # No fallback generation — a missing Session ID is left null so the
    # pre-publish schema check (STORY_KAFKA_SCHEMA/DISCUSSION_KAFKA_SCHEMA
    # both require sessionId) catches and reports it, instead of silently
    # inventing an identifier for a row that never had one.
    session_id = get_csv_value(row_dict, expected_cols, "Session ID")

    program_info = None
    leader_info = None
    tenant_code = "mitra"

    if metadata:
        program_info = metadata.get("programInfo")
        leader_info = metadata.get("LeaderCategoryInfo")
        tenant_code = metadata.get("tenantCode", "mitra")

    designation = None
    if normalized_type == "story":
        designation = get_csv_value(row_dict, expected_cols, "Designation")
    if not designation and leader_info and leader_info.get("name"):
        designation = leader_info["name"].split("(")[0].strip()

    published_at_raw = get_csv_value(row_dict, expected_cols, "Report Created At")
    event_published_at = format_datetime(published_at_raw, with_ms=True)

    user_name = get_csv_value(row_dict, expected_cols, "User name")
    organization = get_csv_value(row_dict, expected_cols, "Organization")
    district = get_csv_value(row_dict, expected_cols, "District")

    state = None
    if normalized_type == "discussion":
        state_raw = get_csv_value(row_dict, expected_cols, "User Location")
    else:
        state_raw = get_csv_value(row_dict, expected_cols, "Location")

    if state_raw:
        state_str = str(state_raw)
        if "," in state_str:
            state_str = state_str.split(",")[-1].strip()
        state = state_str.title()

    user_id = None
    author_val = get_csv_value(row_dict, expected_cols, "Author")
    if normalized_type == "discussion":
        user_id = str(author_val) if author_val is not None else str(submission_id)
    else:
        user_id_val = get_csv_value(row_dict, expected_cols, "userId")
        user_id = str(author_val) if author_val is not None else (
            str(user_id_val) if user_id_val is not None else str(submission_id)
        )

    if normalized_type == "discussion":
        submission_date_raw = get_csv_value(row_dict, expected_cols, "Date of Discussion")
        submission_date = format_datetime(submission_date_raw, with_ms=False)
    else:
        submission_date = format_datetime(published_at_raw, with_ms=False)

    pdf_col = "Pdf" if normalized_type == "story" else "PDF Urls"
    original_pdf = get_url_field(get_csv_value(row_dict, expected_cols, pdf_col))
    pdf_urls = None
    if original_pdf:
        pdf_urls = {"original": original_pdf}

    tags = {
        "state": state,
        "district": district,
        "organization": organization,
        "programId": program_info.get("id") if program_info else None,
        "programName": program_info.get("name") if program_info else None,
        "leaderCategoryId": leader_info.get("id") if leader_info else None,
        "leaderCategoryName": leader_info.get("name") if leader_info else None,
    }

    if normalized_type == "discussion":
        participants_data = []
        role_cols = settings.get_discussion_participants_map()

        total_role = None
        for role, col_name in role_cols.items():
            if role.lower() == "participant count" or (col_name and col_name.lower() == "participant count"):
                total_role = role
                break

        total_count = None
        if total_role:
            col_name = role_cols[total_role]
            if col_name:
                val = get_csv_value(row_dict, expected_cols, col_name)
                if val is not None:
                    try:
                        total_count = int(val)
                    except Exception:
                        pass

        if total_count is not None and total_count > 0:
            participants_data.append({"role": total_role, "count": total_count})

            for role, col_name in role_cols.items():
                if role == total_role or not col_name:
                    continue
                val = get_csv_value(row_dict, expected_cols, col_name)
                if val is not None:
                    try:
                        count = int(val)
                        if count > 0:
                            participants_data.append({"role": role, "count": count})
                    except Exception:
                        pass

        data = {
            "title": get_csv_value(row_dict, expected_cols, "Title"),
            "userId": user_id,
            "userName": user_name,
            "designation": designation,
            "submissionDate": submission_date,
            "imageUrls": parse_csv_list(get_csv_value(row_dict, expected_cols, "Image Urls")),
            "pdfUrls": pdf_urls,
            "transcriptLink": get_csv_value(row_dict, expected_cols, "Transcript Link") or None,
            "challenges": parse_segments(get_csv_value(row_dict, expected_cols, "Challenges")),
            "solutions": parse_segments(get_csv_value(row_dict, expected_cols, "Solutions")),
            "participantsData": participants_data,
            "author": user_id,
            "language": get_csv_value(row_dict, expected_cols, "Language") or "en",
        }
    else:  # story
        data = {
            "title": get_csv_value(row_dict, expected_cols, "Title"),
            "userId": user_id,
            "userName": user_name,
            "designation": designation,
            "submissionDate": submission_date,
            "imageUrls": parse_csv_list(get_csv_value(row_dict, expected_cols, "Images")),
            "pdfUrls": pdf_urls,
            "transcriptLink": get_csv_value(row_dict, expected_cols, "Transcript Link") or None,
            "objective": get_csv_value(row_dict, expected_cols, "Objective"),
            "challenges": parse_segments(get_csv_value(row_dict, expected_cols, "Challenges")),
            "actionSteps": parse_segments(get_csv_value(row_dict, expected_cols, "Action Steps")),
            "impact": get_csv_value(row_dict, expected_cols, "Impact"),
            "duration": get_csv_value(row_dict, expected_cols, "Duration"),
            "blurb": get_csv_value(row_dict, expected_cols, "Blurb"),
            "content": get_csv_value(row_dict, expected_cols, "Content"),
        }

    payload = {
        "submissionId": submission_id,
        "submissionType": report_type,
        "sessionId": session_id,
        "tenantCode": tenant_code,
        "eventType": event_type,
        "eventPublishedAt": event_published_at,
        "tags": tags,
        "data": data,
    }
    return json.dumps(payload, default=str)


def rows_to_json(
    df: pd.DataFrame,
    report_type: str,
    event_type: str = "create",
    metadata: Optional[dict] = None,
):
    for _, row in df.iterrows():
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        is_complete, missing_fields = _is_row_complete(row_dict, report_type)
        if not is_complete:
            logger.warning(
                "Skipping CSV row due to missing required data: %s",
                missing_fields,
            )
            continue
        yield row_to_json(row, report_type, event_type, metadata)


# ---------------------------------------------------------------------------
# Service Orchestration Logic
# ---------------------------------------------------------------------------

async def handle_upload(
    report_type: str,
    program_name: str,
    leader_category: str,
    tenant_code: str,
    file_name: str,
    file_bytes: bytes,
) -> dict:
    from app.temporal.workflows import CsvProcessingWorkflow

    normalized_type = report_type.lower().strip()
    file_size = len(file_bytes)

    # Duplicate check
    is_duplicate = await operations.check_duplicate_file(
        program_name=program_name,
        leader_category=leader_category,
        report_type=normalized_type,
        file_name=file_name,
        file_size=file_size,
    )
    if is_duplicate:
        raise DuplicateFile("FILE ALREADY EXISTS")

    # Validate columns FIRST — reject before touching GCS or the DB, so a
    # malformed CSV never leaves cloud-storage or tracking-table clutter behind.
    try:
        df = await asyncio.to_thread(pd.read_csv, io.BytesIO(file_bytes))
    except Exception as exc:
        raise InvalidCsvColumns([f"Failed to parse CSV: {exc}"])

    is_valid, errors = await asyncio.to_thread(validate_columns, df, normalized_type)
    if not is_valid:
        raise InvalidCsvColumns(errors)

    # Upload to GCS
    try:
        cloud_storage_path = await asyncio.to_thread(upload_csv, file_bytes, normalized_type, file_name)
    except Exception as exc:
        logger.error("GCS Upload failed: %s", exc)
        raise RuntimeError(f"GCS Upload failed: {exc}. Please verify GCS settings.")

    meta_data = {
        "original_filename": file_name,
        "program_name": program_name,
        "leader_category": leader_category,
        "report_type": normalized_type,
        "tenant_code": tenant_code,
    }

    record_id = await operations.insert_upload_record(
        report_type=normalized_type,
        program_name=program_name,
        leader_category=leader_category,
        cloud_storage_path=cloud_storage_path,
        file_name=file_name,
        file_size=file_size,
        meta_data=meta_data,
        status="pending",
    )

    logger.info(
        "CSV uploaded: id=%s, report_type=%s, status=pending, cloud_storage_path=%s",
        record_id, normalized_type, cloud_storage_path,
    )

    # Trigger Temporal workflow in real-time mode
    if settings.PROCESSING_MODE.lower().strip() == "real-time":
        try:
            temporal_client = await Client.connect(settings.TEMPORAL_HOST, namespace=settings.TEMPORAL_NAMESPACE)
            await temporal_client.start_workflow(
                CsvProcessingWorkflow.run,
                record_id,
                id=f"csv-upload-{record_id}",
                task_queue=settings.TEMPORAL_QUEUE,
            )
            logger.info("Triggered real-time CsvProcessingWorkflow for upload ID %s", record_id)
        except Exception as e:
            logger.error("Failed to trigger real-time CsvProcessingWorkflow: %s", e)
            await operations.update_status(record_id, "on_hold", {"error": f"Temporal trigger failed: {e}"})
            raise RuntimeError(f"Failed to start CSV processing workflow: {e}")

    return {
        "message": "Successfully uploaded to cloud",
        "id": record_id,
        "status": "pending",
    }


async def handle_push(record_id: int) -> dict:
    from app.temporal.workflows import CsvProcessingWorkflow

    record = await operations.get_record(record_id)
    if not record:
        raise RecordNotFound("Record not found")

    status = record.get("status")
    if status == "in_progress":
        raise RecordAlreadyProcessing("Record is already being processed")
    if status != "pending":
        raise RecordNotPending("Only pending records can be processed")

    claim_status = await operations.try_claim_for_processing(record_id)
    if claim_status is None:
        raise RecordNotFound("Record not found")
    if claim_status == "in_progress":
        raise RecordAlreadyProcessing("Record is already being processed")

    try:
        temporal_client = await Client.connect(settings.TEMPORAL_HOST, namespace=settings.TEMPORAL_NAMESPACE)
        await temporal_client.start_workflow(
            CsvProcessingWorkflow.run,
            record_id,
            id=f"csv-upload-{record_id}",
            task_queue=settings.TEMPORAL_QUEUE,
        )
        return {"status": "success", "message": "CSV processing workflow started"}
    except Exception as e:
        await operations.update_status(record_id, "on_hold", {"error": str(e)})
        raise RuntimeError(f"Failed to start CSV processing workflow: {e}")
