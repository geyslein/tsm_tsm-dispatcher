import logging
import json
import os.path

from AvroSchemaValidator import validate_avro_schema
from fastavro._validate_common import ValidationError


def on_message(client, userdata, message):
    logging.info(
        f"received message {message.mid} "
        f"on topic '{message.topic}' with QoS {message.qos}"
    )
    logging.debug(f"{message=}")

    callback = userdata["act"]
    schema_file = userdata["schema_file"]

    try:
        decoded: str = message.payload.decode("utf-8")

        if schema_file is None:
            try:
                # also parse single numeric values
                # and the constants null, +/-Infinity, NoN
                content = json.loads(decoded)
            except json.JSONDecodeError:
                # string / datetime / other
                content = decoded

        # we have an avro schema to check against
        else:
            name = os.path.basename(schema_file)
            content = json.loads(decoded)
            if not validate_avro_schema(content, schema_file):
                raise ValueError(f"Received message does not match avro schema '{name}'")
            logging.debug(f"Received message matches avro schema '{name}'")

        if isinstance(content, dict):
            content["topic"] = message.topic

        callback(content)

    except Exception as e:
        logging.error(f"Errors occurred. discarding message {message.mid}", exc_info=e)


def on_log(client, userdata, level, buf):
    logging.debug(f"{buf}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(f"Connected to {userdata['mqtt_broker']}!")
    else:
        logging.error(f"Failed to connect, return code {rc}\n")
