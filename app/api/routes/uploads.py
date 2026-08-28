import logging
from fastapi import APIRouter, Depends, Form, UploadFile, File, BackgroundTasks
from fastapi.security import HTTPAuthorizationCredentials

from app.api.deps import verify_auth_token
from app.api.validators.uploads import (
    validate_report_type,
    validate_extension,
    validate_file_bytes,
)
from app.api.models.uploads import UploadResponse
from app.api.services import uploads as upload_service
from app.config import settings

logger = logging.getLogger("analytics_service.api.routes.uploads")

uploads_router = APIRouter(prefix="/v1", tags=["CSV Pipeline"])


@uploads_router.post("/upload/", response_model=UploadResponse)
async def upload_report(
    background_tasks: BackgroundTasks,
    report_type: str = Form(...),
    program_name: str = Form(...),
    leader_category: str = Form(...),
    tenant_code: str = Form(default="mitra"),
    file: UploadFile = File(...),
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Upload a CSV report file.

    The file is stored in GCS and a tracking record is created with
    status='pending'. Processing (column validation, Kafka publishing)
    runs in the background.
    """
    # Pure request-shape checks
    report_type = validate_report_type(report_type)
    validate_extension(file.filename)

    file_bytes = await file.read(settings.MAX_CSV_UPLOAD_BYTES + 1)
    validate_file_bytes(file_bytes)

    # Delegate to domain service
    return await upload_service.handle_upload(
        report_type=report_type,
        program_name=program_name,
        leader_category=leader_category,
        tenant_code=tenant_code,
        file_name=file.filename,
        file_bytes=file_bytes,
        background_tasks=background_tasks,
    )


@uploads_router.post("/process/csv/{record_id}")
async def push_record(
    record_id: int,
    background_tasks: BackgroundTasks,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Manually trigger processing for a specific csv_upload record in pending status.
    """
    return await upload_service.handle_push(record_id, background_tasks)

