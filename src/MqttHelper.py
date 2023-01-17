import logging
import json
from AvroSchemaValidator import validate_avro_schema
from fastavro._validate_common import ValidationError


def on_message(client, userdata, message):
    logging.info(f"Received message on topic '{message.topic}' with QoS {message.qos}")

    callback = userdata["act"]
    schema_file = userdata["schema_file"]
    try:
        payload = str(message.payload.decode("utf-8"))
        content = json.loads(payload)
    except json.JSONDecodeError as e:
        logging.error(f"message parsing failed with: {type(e).__name__}: {e}")
        return

    if schema_file is None:
        pass
    elif validate_avro_schema(content, schema_file):
        # must be an avro proofed messages, otherwise we
        # might overwrite a user-set 'topic'
        content["topic"] = message.topic
    else:
        logging.error("avro schema validation failed: discarding message")
        return

    try:
        callback(content)
    except Exception as e:
        logging.error(
            f"message processing failed with: {type(e).__name__}:{e}\n"
            f"original message: {payload}"
        )


def on_log(client, userdata, level, buf):
    logging.debug(f"{buf}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(f"Connected to {userdata['mqtt_broker']}!")
    else:
        logging.error(f"Failed to connect, return code {rc}\n")
