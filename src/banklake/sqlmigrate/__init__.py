"""Legacy (Teradata-style) SQL -> Athena (Trino) SQL migration framework.

Deliberately not "AI converts anything": a tokenizer-based, rule-driven converter for a defined subset. Anything
outside that subset is reported as UNSUPPORTED instead of producing wrong SQL. Components:
parser (tokenizer), functions, datatypes, schema (name mapping), unsupported (detector), validator (Athena
syntax check), comparator (result comparison) and runner (execute + compare against PySpark references).
"""
from .comparator import compare, summarize
from .converter import Conversion, convert
from .runner import EXAMPLES, REFERENCES, duckdb_shim, run_all, validate
from .schema import SchemaMapper
from .validator import check_syntax

__all__ = ["Conversion", "convert", "run_all", "validate", "compare", "summarize", "SchemaMapper", "check_syntax",
           "EXAMPLES", "REFERENCES", "duckdb_shim"]
