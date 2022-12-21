TSM Dispatcher is a service to handle a collection of actions when they are triggered by MQTT events.

# Install

Requirements:
```
pip install -r requirements.txt
```

# @Todo

- [ ] Add authentication
- [ ] Handle different action types (add dynamic class loader for action
      implementations like in tsm extractor)
- [ ] Add options for actions (like Minio admin credentials for
      `CreateThingOnMinioAction`
- [ ] Maybe handle all action types in one process (with threads) or use
      something more sophisticated (Node Red?)
- [ ] Security: No user should be able to modify bucket tags as they are
      used to detect the things uuid

# Developer Howto

1. Checkout the tsm orchestration repo and switch into the directory:
      
    ```shell
    git clone git@git.ufz.de:rdm-software/timeseries-management/tsm-orchestration.git
    cd tsm-orchestration
    ```
      
2. Start all the tsm components with docker compose and check if they are running fine:
      
    ```shell
    docker-compose up -d
    docker-compose ps
                         Name                                    Command                  State                                       Ports                                 
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    tsm-orchestration_basic-demo-scheduler_1          python3 main.py --mqtt-bro ...   Up                                                                                   
    tsm-orchestration_database_1                      docker-entrypoint.sh postgres    Up (healthy)   0.0.0.0:5432->5432/tcp,:::5432->5432/tcp                              
    tsm-orchestration_frontend_1                      bash -c python3 manage.py  ...   Up                                                                                   
    tsm-orchestration_mqtt-broker_1                   /docker-entrypoint.sh /usr ...   Up (healthy)   127.0.0.1:1883->1883/tcp, 1884/tcp, 127.0.0.1:8883->8883/tcp          
    tsm-orchestration_mqtt-cat_1                      /docker-entrypoint.sh mosq ...   Up             1883/tcp                                                              
    tsm-orchestration_object-storage_1                /usr/bin/docker-entrypoint ...   Up (healthy)   9000/tcp                                                              
    tsm-orchestration_proxy_1                         /docker-entrypoint.sh ngin ...   Up             127.0.0.1:443->443/tcp, 127.0.0.1:80->80/tcp, 127.0.0.1:9000->9000/tcp
    tsm-orchestration_tsmdl_1                         /app/start.sh                    Up                                                                                   
    tsm-orchestration_visualization_1                 /run.sh                          Up             3000/tcp                                                              
    tsm-orchestration_worker-db-setup_1               python3 main.py --topic th ...   Up                                                                                   
    tsm-orchestration_worker-file-ingest_1            python3 main.py --topic ob ...   Up                                                                                   
    tsm-orchestration_worker-mqtt-ingest_1            python3 main.py --topic mq ...   Up                                                                                   
    tsm-orchestration_worker-mqtt-user-creation_1     python3 main.py --topic th ...   Up                                                                                   
    tsm-orchestration_worker-object-storage-setup_1   python3 main.py --topic th ...   Up                                                                                   
    tsm-orchestration_worker-run-qaqc_1               python3 main.py --topic da ...   Up 
    ```
   
3. Stop the specific worker container who is running the dispatcher action you like to edit

    ```shell
    # When you like to edit the `CreateThingOnMinioAction`
    docker-compose stop worker-mqtt-ingest
    ```
   
4. Checkout the [tsm-dispatcher](https://git.ufz.de/rdm-software/timeseries-management/tsm-dispatcher)) repo and switch to the directory:
    ```shell
    cd ..
    git clone git@git.ufz.de:rdm-software/timeseries-management/tsm-dispatcher.git
    cd tsm-dispatcher
    ```

5. Run the specific dispatcher action from your local development environment, i.e. from PyCharm 
   and with your debugger or from a container.

    ```shell
    # Running it local from container
    docker run --rm --net=host -v `pwd`:/home/appuser/app/ git.ufz.de:4567/rdm-software/timeseries-management/tsm-dispatcher/dispatcher:latest -v --topic mqtt_ingest/# --mqtt-broker localhost:1883 -u mqtt -p mqtt parse-data --target-uri postgresql://postgres:postgres@localhost/postgres
    # Two things to be aware of:
    # 1. You have to restart the container to apply changes of your source files
    # 2. Docker uses `--net=host` to allow network connections to the database and mqtt broker 
    #    which are provided by the tsm-orchestration docker-compose setup. The container network 
    #    is not isolated from your host! 
    ```