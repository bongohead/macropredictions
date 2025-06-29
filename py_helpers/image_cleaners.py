"""
Functions for resizing/clipping images - useful for multimodal LLM scraping
"""

import base64
import io
from PIL import Image
import warnings

def clip_height(b64: str, max_height: int):
    """
    Crop an image to a height limit of `max_height` pixels (throws out the bottom). If the 
     image is already ≤ `max_height`, its returned unchanged.

    Params:
        @b64: A base-64 string, with no data-URI component.
        @max_height: The desired height to retain.

    Returns:
        A b64 string.
    """
    raw = base64.b64decode(b64)
    with Image.open(io.BytesIO(raw)) as im:
        w, h = im.size
        if h <= max_height:
            return b64

        clipped = im.crop((0, 0, w, max_height)) # (l, t, l, b)
        buf = io.BytesIO()
        clipped.save(buf, format = im.format)
        return base64.b64encode(buf.getvalue()).decode()
    
    
def resize_image(b64, width: int | None = None, quality: int | None = None, output_format: str | None = None):
    """
    Resize/compress/change format of an image

    Params:
        @b64: The base64 string.
        @width: The target width; if >= original width, 0, or None, the image dimensions are kept.
        @quality: The target quality; if 0 or None, the original JPEG's quality setting is re-used.
        @output_format: One of 'png', 'webp', 'jpeg', or None. If None, the original format is used.

    Returns:
        A dict with keys 'image', 'width', 'height', 'quality'

    Examples:
        thumb = resize_image(src_b64, quality = 60) 1) Keep size, reduce quality/file size
        smaller = resize_image(src_b64, width = 600, quality = 60) # 2) Downscale + reduce quality
        smaller_no_recompress = resize_image(src_b64, width = 600) # 3) Pure resize while maintain quality
        unchanged = resize_image(src_b64) # 4) Do absolutely nothing, return original bytes with measurements
    """
    width = None if width in (None, 0) else int(width)
    quality = None if quality in (None, 0) else int(quality)

    raw = base64.b64decode(b64)
    with Image.open(io.BytesIO(raw)) as im:
        src_format = im.format.upper()
        w0, h0 = im.size

        # ---------- Resize if needed ----------
        if width is None or width >= w0:
            new_w, new_h, img_to_save = w0, h0, im
        else:
            ratio = h0 / w0
            new_w = width
            new_h = int(new_w * ratio)
            img_to_save = im.resize((new_w, new_h), Image.LANCZOS)

        # ---------- Choose output format - JPEG/WEBP/PNG ----------
        fmt = (output_format or src_format).upper()
        if fmt not in ('JPEG', 'WEBP', 'PNG'):
            raise ValueError(f"Unsupported output_format '{output_format}'")
        
        if fmt == 'PNG' and quality is not None:
            warnings.warn("'quality' is ignored for PNG, ignoring.")
            quality = None

        # ---------- Re-encode if needed ----------
        must_reencode = (
            img_to_save is not im # If resized
            or quality is not None # OR quality changed
            or fmt != src_format # OR format changed
        )
        if not must_reencode:
            return {
                'image': b64,
                'width': w0,
                'height': h0,
                'quality': im.info.get('quality'),
                'format': src_format,
            }

        buf = io.BytesIO()
        save_kwargs = {}
        if fmt in ('JPEG', 'WEBP') and quality is not None:
            save_kwargs['quality'] = quality
        if fmt == 'WEBP' and quality is None:
            save_kwargs['lossless'] = True

        img_to_save.save(buf, format = fmt, **save_kwargs)
        new_b64 = base64.b64encode(buf.getvalue()).decode()

    return {
        'image': new_b64,
        'width': new_w,
        'height': new_h,
        'quality': save_kwargs.get('quality'),
        'format': fmt
    }