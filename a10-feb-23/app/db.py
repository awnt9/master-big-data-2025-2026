import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from app.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def get_connection_params():
    return {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "dbname": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }


@contextmanager
def get_cursor():
    conn = psycopg2.connect(**get_connection_params())
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conn.close()


def insert_image(cursor, image_name, bucket, height, width, size_bytes, fmt, s3_link):
    cursor.execute(
        """
        INSERT INTO images (image_name, bucket, height, width, size_bytes, format, s3_link)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (bucket, image_name) DO UPDATE SET
            height = EXCLUDED.height,
            width = EXCLUDED.width,
            size_bytes = EXCLUDED.size_bytes,
            format = EXCLUDED.format,
            s3_link = EXCLUDED.s3_link,
            upload_date = CURRENT_TIMESTAMP
        RETURNING id
        """,
        (image_name, bucket, height, width, size_bytes, fmt, s3_link),
    )
    return cursor.fetchone()["id"]


def get_all_images(cursor):
    cursor.execute(
        """
        SELECT id, image_name, bucket, height, width, size_bytes, format, upload_date, s3_link
        FROM images
        ORDER BY upload_date DESC
        """
    )
    return cursor.fetchall()


def get_image_by_id(cursor, image_id: int):
    cursor.execute(
        """
        SELECT id, image_name, bucket, height, width, size_bytes, format, upload_date, s3_link
        FROM images
        WHERE id = %s
        """,
        (image_id,),
    )
    return cursor.fetchone()


def delete_image(cursor, image_id: int):
    cursor.execute("DELETE FROM images WHERE id = %s RETURNING id", (image_id,))
    return cursor.fetchone() is not None
