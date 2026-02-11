"""
Tests for CSV encoding auto-detection in src/utils/encoding.py.

Covers UTF-8 detection, latin-1 detection, and empty-file fallback.
"""

import codecs

import pytest

from src.utils.encoding import detect_csv_encoding


def _can_decode(encoding: str, data: bytes) -> bool:
    """Return True if *data* decodes cleanly with *encoding*."""
    try:
        data.decode(encoding)
        return True
    except (UnicodeDecodeError, LookupError):
        return False


class TestDetectCsvEncodingUtf8:
    """Verify detection of UTF-8 encoded files."""

    def test_utf8_file_detected(self, tmp_path):
        """A file written as UTF-8 should be detected as utf-8 or ascii."""
        csv_file = tmp_path / "utf8.csv"
        csv_file.write_text("name,city\nAlice,Zürich\n", encoding="utf-8")

        result = detect_csv_encoding(str(csv_file))
        assert result in ("utf-8", "ascii"), f"Expected utf-8 or ascii, got {result}"


class TestDetectCsvEncodingLatin:
    """Verify detection of latin-1 / Windows-1252 encoded files."""

    def test_latin1_file_detected(self, tmp_path):
        """A file with latin-1 bytes should be detected as a latin-compatible encoding."""
        csv_file = tmp_path / "latin.csv"
        # Write a realistic latin-1 CSV with enough content for reliable detection
        content = (
            b"name,stadt,beschreibung\n"
            b"M\xfcller,K\xf6ln,Gesch\xe4ftsf\xfchrer\n"
            b"Gro\xdf,D\xfcsseldorf,\xdcbersetzerin\n"
            b"Stra\xdfe,N\xfcrnberg,B\xe4ckerei\n"
        )
        csv_file.write_bytes(content)

        result = detect_csv_encoding(str(csv_file))
        # The detected encoding must be able to decode the original bytes
        assert _can_decode(result, content), (
            f"Detected encoding {result} cannot decode the latin-1 content"
        )
        # It must not be utf-8 (which would fail on these bytes)
        assert result != "utf-8", (
            f"Expected a non-UTF-8 encoding for latin-1 bytes, got {result}"
        )


class TestDetectCsvEncodingEmptyFile:
    """Verify fallback for empty files."""

    def test_empty_file_returns_utf8(self, tmp_path):
        """An empty file should fall back to utf-8."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_bytes(b"")

        result = detect_csv_encoding(str(csv_file))
        assert result == "utf-8", f"Expected utf-8 fallback, got {result}"
