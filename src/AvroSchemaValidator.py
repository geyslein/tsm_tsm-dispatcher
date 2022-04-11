import logging

from fastavro.schema import load_schema
from fastavro import validate
from fastavro._validate_common import ValidationError

def validate_avro_schema(message, has_schema, schema_file):
    if has_schema:
        schema = load_schema(schema_file)
        try:
            validate(message, schema)
            logging.info("Received message matches schema!")
            return True
        except ValidationError:
            logging.warning("Received message does not match given avro schema!")
            return False
    else:
        return True