"""Data-type converter: Teradata type names used inside CAST(x AS <type>) -> Athena (Trino) types."""
import re

SIMPLE = {"BYTEINT": "TINYINT", "SMALLINT": "SMALLINT", "INT": "INTEGER", "INTEGER": "INTEGER", "BIGINT": "BIGINT",
          "FLOAT": "DOUBLE", "REAL": "DOUBLE", "DOUBLE PRECISION": "DOUBLE", "DATE": "DATE", "CLOB": "VARCHAR",
          "BYTE": "VARBINARY", "VARBYTE": "VARBINARY", "BLOB": "VARBINARY", "JSON": "JSON", "BOOLEAN": "BOOLEAN"}


class Unsupported(Exception):
    pass


def convert_type(text: str):
    """Returns (athena_type, note). Raises Unsupported for types with no safe equivalent."""
    t = re.sub(r"\s+", " ", text.strip().upper())
    if "FORMAT" in t:
        raise Unsupported("FORMAT phrase inside a type")
    note = None
    if "CHARACTER SET" in t:
        t = re.sub(r"\s*CHARACTER SET\s+\w+", "", t)
        note = "CHARACTER SET clause dropped (Athena strings are Unicode)"
    if t.startswith(("INTERVAL", "PERIOD")):
        raise Unsupported(f"type {t.split('(')[0].split()[0]}")
    m = re.fullmatch(r"(?:DECIMAL|DEC|NUMERIC|NUMBER)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", t)
    if m:
        return f"DECIMAL({m.group(1)}, {m.group(2) or 0})", note
    if t in ("DECIMAL", "DEC", "NUMERIC"):
        return "DECIMAL(5, 0)", "Teradata default precision DECIMAL(5,0) made explicit"
    if t == "NUMBER":
        raise Unsupported("NUMBER without precision")
    m = re.fullmatch(r"(VARCHAR|CHAR|CHARACTER)(?:\s*\(\s*(\d+)\s*\))?", t)
    if m:
        base = "VARCHAR" if m.group(1) == "VARCHAR" else "CHAR"
        return (f"{base}({m.group(2)})" if m.group(2) else base), note
    m = re.fullmatch(r"(TIMESTAMP|TIME)\s*(?:\(\s*\d+\s*\))?(\s+WITH TIME ZONE)?", t)
    if m:
        return m.group(1) + (m.group(2) or ""), "fractional-second precision dropped" if "(" in t else note
    if t in SIMPLE:
        return SIMPLE[t], note
    raise Unsupported(f"type {t}")
