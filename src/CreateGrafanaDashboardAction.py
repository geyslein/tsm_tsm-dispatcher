# import xml.etree.ElementTree as ET
import os
from grafana_client import GrafanaApi
import psycopg2
from psycopg2 import sql as psysql
import json
import logging



from AbstractAction import AbstractAction, MQTTMessage
from thing import Thing

class CreateGrafanaDashboardAction(AbstractAction):

    def __init__(
        self, topic, mqtt_broker, mqtt_user, mqtt_password,
        database_settings:dict, grafana_settings:dict):

        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db = psycopg2.connect(database_settings.get('url'))
        self.grafana = GrafanaApi.from_url(
                url=grafana_settings.get('url'),
                credential=(grafana_settings.get('user'),
                            grafana_settings.get('password'))
                )


    def act(self, content: dict, message: MQTTMessage):
        thing = Thing.get_instance(content)

        # create datasource if not exists
        self.create_datasource(thing)
        # create folder if not exists
        self.create_folder(thing)
        # create or update dashboard
        self.create_dashboard(thing)


    def create_datasource(self, thing):
        # create datasource if it doesn't exist
        if not self.check_datasource_exists(thing):
            datasource = self.new_datasource(thing)
            try:
                self.grafana.datasource.create_datasource(datasource)
            except Exception as e:
                self.logger.warning(f"Cannot create datasource.\n'{e}'")

    def check_datasource_exists(self, thing):
        # check if datasource with project uuid exists
        datasource_uid = thing.project.uuid
        try:
            self.grafana.datasource.get_datasource_by_uid(datasource_uid)
            return True
        except:
            return False

    def new_datasource(self, thing):
        # define datasource dictionary
        ds_uid = thing.project.uuid
        ds_name = thing.project.name
        db_user = thing.database.username.lower()
        db_password = thing.database.password
        datasource={
            "name": ds_name,
            "uid": ds_uid,
            "type":"postgres",
            "url": "database:5432",
            "user": db_user,
            "access": "proxy",
            "basicAuth": False,
            "jsonData" : {
                "database": "postgres",
                "sslmode": "disable",
                "timescaledb": True
            },
            "secureJsonData": {
                "password": db_password
            }
        }
        return datasource


    def create_folder(self, thing):
        # create folder if it doesn't exist
        folder_uid = thing.project.uuid
        folder_title = thing.project.name
        if not self.check_folder_exists(thing):
            try:
                self.grafana.folder.create_folder(folder_title, folder_uid)
            except Exception as e:
                self.logger.warning(f"Can't create folder.\n'{e}")

    def check_folder_exists(self, thing):
        # check if folder with project uuid exists
        folder_uid = thing.project.uuid
        try:
            self.grafana.folder.get_folder(folder_uid)
            return True
        except Exception:
            return False


    def create_dashboard(self, thing, overwrite=True):
        # create/update dashboard if it doesn't exist or overwrite is True
        if not self.check_dashboard_exists(thing) or overwrite==True:
            dashboard = self.build_dashboard_dict(thing)
            try:
                self.grafana.dashboard.update_dashboard(dashboard)
            except Exception as e:
                self.logger.warning(f"Cannot create dashboard.\n'{e}")

    def check_dashboard_exists(self, thing):
        # check if dashboard with thing uuid exists
        dashboard_uid = thing.uuid
        try:
            self.grafana.dashboard.get_dashboard(dashboard_uid)
            return True
        except Exception as e:
            return False

    def build_dashboard_dict(self, thing):
        # get list of panel dictionaries for dashboard
        panels = self.build_panels_dict(thing)
        dashboard_uid = thing.uuid
        dashboard_title = thing.name
        folder_uid = thing.project.uuid
        folder_title = thing.project.name
        # build dashboard dictionary
        dashboard = {
            "dashboard":{
                "annotations": {
                    "list": []
                },
                "editable": True,
                "fiscalYearStartMonth": 0,
                "graphTooltip": 0,
                "links": [],
                "liveNow": True,
                "panels": panels,
                "refresh": False,
                "style": "dark",
                "tags": [
                    folder_title,
                    dashboard_title,
                    "TSM_automation"
                ],
                "templating": {
                    "list": []
                },
                "time":{
                    "from": "now-90d",
                    "to": "now"
                },
                "timepicker": {},
                "timezone": "",
                "title": dashboard_title,
                "uid": dashboard_uid
            },
            "folderUid": folder_uid,
            "message": "created by TSM dashboard automation",
            "overwrite": True
        }
        return dashboard

    def build_panels_dict(self, thing, width=8, height=6, ncol=3):
        # each datastream is plotted into one panel
        db_user = thing.database.username.lower()
        ds_uid = thing.project.uuid
        # query all datastreams for thing from database
        datastreams = self.get_datastreams(thing)
        panels = []
        # loop over datastreams, create list of panel dictionaries
        # to be used in dashboard dictionary
        for num, (ds_id, ds_name) in enumerate(datastreams):
            (x,y) = self.panel_position(num, width, height, ncol)
            sql_query = "SELECT result_time AS \"time\", result_number "\
                        f"AS \"value\" FROM {db_user}.observation "\
                        f"WHERE datastream_id = {ds_id}"
            # append panel dictionary to panels list
            panels.append({
                "id": num + 1,
                "title": ds_name,
                "type": "timeseries",
                "datasource": {
                    "type": "postgres",
                    "uid": ds_uid
                },
                "gridPos": {
                    "h": height,
                    "w": width,
                    "x": x,
                    "y": y
                },
                "targets": [{
                    "datasource": {
                        "type": "postgres",
                        "uid": ds_uid
                    },
                    "editorMode": "code",
                    "format": "time_series",
                    "rawQuery": True,
                    "rawSql": sql_query,
                    "refId": "A"
                }]
            })
        return panels

    def get_datastreams(self, thing):
        user = thing.database.username.lower()
        with self.db:
            with self.db.cursor() as c:
                c.execute(
                    psysql.SQL(
                        "SELECT id, name FROM {user}.datastream"
                        ).format(user=psysql.Identifier(user)
                    )
                )
                return c.fetchall()

    @staticmethod
    def panel_position(num, width, height, ncol):
        x = (num % ncol) * width
        y = (num // ncol) * height
        return (x, y)