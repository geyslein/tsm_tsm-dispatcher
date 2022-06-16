import logging
import json
from AvroSchemaValidator import validate_avro_schema
from fastavro._validate_common import ValidationError


TOPIC_DELIMITER = '/'

def on_message(client, userdata, message):
    content = str(message.payload.decode("utf-8"))
    parsed_content = json.loads(content)
    if validate_avro_schema(parsed_content, userdata['schema_file']):
        logging.info(
            "Received message on topic '{topic}' with QoS {qos}!".format(topic=message.topic, qos=message.qos))
        try:
            userdata['act'](parsed_content, client, userdata, message)
        except Exception as e:
            logging.warning("Error during message processing: {err}!".format(err=e))
            logging.warning("The following message could not be processed:")
            logging.warning(parsed_content)
    else:
        logging.warning("Schema of received message does not match with given avro schema!")


def on_log(client, userdata, level, buf):
    logging.info("{}".format(buf))


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to {mqtt_broker}!".format(mqtt_broker=userdata['mqtt_broker']))
    else:
        logging.info("Failed to connect, return code %d\n", rc)


def get_schema_name_from_topic(topic):
    return topic.split(TOPIC_DELIMITER)[0]


def get_device_id_from_topic(topic):
    return topic.split(TOPIC_DELIMITER)[1]
