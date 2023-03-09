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
        tree = ET.parse("./tomcat/tomcat_context_template.xml")
        root = tree.getroot()
        xml_string = ET.tostring(root,encoding="utf8", method="xml").decode()
        tomcat_context = xml_string.format(db_url=thing.database.url,
                                               schema=schema,
                                               username=thing.database.username,
                                               password=thing.database.password)
        xml_tree = ET.XML(tomcat_context)
        with open(f"./tomcat/context_files/FROST-Server#{schema}.xml", "wb") as file:
            file.write(ET.tostring(xml_tree))
