"""CSV encoding auto-detection via charset-normalizer."""

import codecs
import logging

from charset_normalizer import from_path

logger = logging.getLogger(__name__)

_FALLBACK = "utf-8"


def _normalize_encoding_name(name: str) -> str:
    """Return the canonical Python codec name (e.g. 'utf-8', not 'utf_8')."""
    try:
        return codecs.lookup(name).name
    except LookupError:
        return name


def detect_csv_encoding(file_path: str) -> str:
    """
    Detect the encoding of a CSV file.

    Uses charset-normalizer to analyse the file bytes and return the
    most likely encoding.  Falls back to ``"utf-8"`` when detection is
    inconclusive or raises an error.

    Args:
        file_path: Path to the CSV file to analyse.

    Returns:
        Detected encoding string (e.g. ``"utf-8"``, ``"cp1252"``).
    """
    try:
        result = from_path(file_path)
        best = result.best()
        if best is None:
            logger.info(
                "Encoding detection inconclusive for %s, falling back to %s",
                file_path,
                _FALLBACK,
            )
            return _FALLBACK
        encoding = _normalize_encoding_name(str(best.encoding))
        logger.info("Detected encoding for %s: %s", file_path, encoding)
        return encoding
    except Exception:
        logger.warning(
            "Encoding detection failed for %s, falling back to %s",
            file_path,
            _FALLBACK,
            exc_info=True,
        )
        return _FALLBACK
