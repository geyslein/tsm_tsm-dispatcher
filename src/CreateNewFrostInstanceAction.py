import xml.etree.ElementTree as ET

from AbstractAction import AbstractAction, MQTTMessage
from thing import Thing


class CreateNewFrostInstanceAction(AbstractAction):

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, database_settings: dict):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)

    def act(self, content: dict, message: MQTTMessage):
        thing = Thing.get_instance(content)

        self.create_xml_object(thing)

    def create_xml_object(self, thing):
        schema = thing.database.username.lower()
        with open("./tomcat/context_xml_template.txt", "r") as file:
            tomcat_context = file.read().replace("\n", "")
        tomcat_context = tomcat_context.format(db_url=thing.database.url,
                                               schema=schema,
                                               username=thing.database.username,
                                               password=thing.database.password)
        xml_tree = ET.XML(tomcat_context)
        with open(f"./tomcat/context_files/{schema}_context.xml", "w+") as file:
            file.write(ET.tostring(xml_tree))
