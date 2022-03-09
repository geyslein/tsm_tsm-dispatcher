import logging

from fastavro.schema import load_schema
from fastavro import validate
from fastavro._validate_common import ValidationError


def avro_schema_validator(schema_file, msg):
    schema = load_schema(schema_file)

    try:
        validate(msg, schema)
        logging.info("message matches schema")
        return True
    except ValidationError:
        logging.warning("message does not match given avro schema")
        return False

