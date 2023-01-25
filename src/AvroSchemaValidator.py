import logging

from fastavro.schema import load_schema
from fastavro import validate
from fastavro._validate_common import ValidationError


def validate_avro_schema(message, schema_file):
    if schema_file is None:
        raise TypeError(f"schema_file cannot be None")
    return validate(message, load_schema(schema_file), raise_errors=False)
