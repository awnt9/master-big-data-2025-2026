import io
import numpy as np
from PIL import Image


def adjust_brightness(image_bytes: bytes, factor: float) -> bytes:
    img = Image.open(io.BytesIO(image_bytes))
    img_array = np.array(img)
    adjusted = np.clip(img_array.astype(np.float64) * factor, 0, 255).astype(np.uint8)
    result = Image.fromarray(adjusted)
    buffer = io.BytesIO()
    result.save(buffer, format=img.format or "JPEG", quality=95)
    return buffer.getvalue()
