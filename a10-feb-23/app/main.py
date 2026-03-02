import io
import csv
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse

from app.config import MINIO_ENDPOINT
from app.minio_client import (
    get_minio_client,
    ensure_bucket_exists,
    upload_object,
    download_object,
    delete_object as minio_delete_object,
)
from app.db import (
    get_cursor,
    insert_image,
    get_all_images,
    get_image_by_id,
    delete_image as db_delete_image,
)
from app.image_processor import adjust_brightness

app = FastAPI(title="InfraDron Image Platform", version="1.0.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload-image")
def upload_image(
    file: UploadFile = File(...),
    bucket: str = Form(..., description="User/bucket identifier"),
):
    if not file.filename or not file.filename.lower().endswith(
        (".jpg", ".jpeg", ".png", ".gif", ".webp")
    ):
        raise HTTPException(400, "Invalid image format. Use jpg, png, gif or webp.")

    content = file.file.read()
    size_bytes = len(content)

    from PIL import Image

    img = Image.open(io.BytesIO(content))
    width, height = img.size
    fmt = img.format or "JPEG"

    client = get_minio_client()
    ensure_bucket_exists(client, bucket)

    object_key = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
    s3_link = upload_object(
        client, bucket, object_key, content, content_type=f"image/{fmt.lower()}"
    )

    with get_cursor() as cursor:
        image_id = insert_image(
            cursor, file.filename, bucket, height, width, size_bytes, fmt, s3_link
        )

    return {
        "id": image_id,
        "image_name": file.filename,
        "bucket": bucket,
        "height": height,
        "width": width,
        "size_bytes": size_bytes,
        "format": fmt,
        "s3_link": s3_link,
    }


@app.get("/images")
def list_images():
    with get_cursor() as cursor:
        rows = get_all_images(cursor)
    return [
        {
            "id": r["id"],
            "image_name": r["image_name"],
            "bucket": r["bucket"],
            "height": r["height"],
            "width": r["width"],
            "size_bytes": r["size_bytes"],
            "format": r["format"],
            "upload_date": r["upload_date"].isoformat() if r["upload_date"] else None,
            "s3_link": r["s3_link"],
        }
        for r in rows
    ]


@app.get("/export-csv")
def export_csv():
    with get_cursor() as cursor:
        rows = get_all_images(cursor)

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            ["id", "image_name", "bucket", "height", "width", "size_bytes", "format", "upload_date", "s3_link"]
        )
        for r in rows:
            writer.writerow(
                [
                    r["id"],
                    r["image_name"],
                    r["bucket"],
                    r["height"],
                    r["width"],
                    r["size_bytes"],
                    r["format"],
                    r["upload_date"].isoformat() if r["upload_date"] else "",
                    r["s3_link"],
                ]
            )
        buffer.seek(0)
        yield buffer.getvalue()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=images_export.csv"},
    )


@app.post("/process-image/{image_id}")
def process_image(image_id: int, factor: float = 1.2):
    if factor not in (0.8, 1.2):
        raise HTTPException(400, "factor must be 0.8 or 1.2")

    with get_cursor() as cursor:
        img_row = get_image_by_id(cursor, image_id)
    if not img_row:
        raise HTTPException(404, f"Image {image_id} not found")

    client = get_minio_client()
    object_key = img_row["s3_link"].split("/")[-1]
    image_bytes = download_object(client, img_row["bucket"], object_key)

    processed_bytes = adjust_brightness(image_bytes, factor)

    from PIL import Image

    img = Image.open(io.BytesIO(processed_bytes))
    fmt = img.format or "JPEG"
    processed_key = f"processed_{factor}_{object_key}"

    s3_link = upload_object(
        client,
        img_row["bucket"],
        processed_key,
        processed_bytes,
        content_type=f"image/{fmt.lower()}",
    )

    return {
        "original_id": image_id,
        "factor": factor,
        "processed_s3_link": s3_link,
    }


@app.delete("/images/{image_id}")
def delete_image(image_id: int):
    with get_cursor() as cursor:
        img_row = get_image_by_id(cursor, image_id)
        if not img_row:
            raise HTTPException(404, f"Image {image_id} not found")

        object_key = img_row["s3_link"].split("/")[-1]
        minio_delete_object(get_minio_client(), img_row["bucket"], object_key)
        deleted = db_delete_image(cursor, image_id)

    if not deleted:
        raise HTTPException(500, "Failed to delete")
    return {"deleted": image_id}
