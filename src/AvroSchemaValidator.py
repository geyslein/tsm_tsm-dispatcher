import logging

from fastavro.schema import load_schema
from fastavro import validate
from fastavro._validate_common import ValidationError


def validate_avro_schema(message, schema_file):
    if not schema_file:
        return False

    schema = load_schema(schema_file)
    name = schema.get('name', 'nameless schema')
    try:
        validate(message, schema)
    except ValidationError:
        logging.warning(f"Received message does not match avro schema '{name}'")
        return False
    else:
        logging.debug(f"Received message matches avro schema '{name}'")
        return True
