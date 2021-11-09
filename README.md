TSM Dispatcher is a service to handle a collection of actions when they are triggered by Apache Kafka events.

# Install

Requirements:
```
pip install git+ssh://git@git.ufz.de/rdm-software/timeseries-management/minio-cli-wrapper.git
pip install -r requirements.txt
```

# Produce (Test-/Debug-) Event

```bash
python main.py --topic thing_created -k kafka:9092 produce "{\"uuid\":\"057d8bba-40b3-11ec-a337-125e5a40a845\",\"name\":\"Axel F.\"}"
```

# Consume (and dispatch)

## Create *thing* on MinIO
```bash
python main.py --topic thing_created -k kafka:9092 -v run-create-thing-on-minio-action-service
```

# @Todo

- [ ] Add authentication
- [ ] Handle different action types (add dynamic class loader for action
      implementations like in tsm extractor)
- [ ] Add options for actions (like Minio admin credentials for
      `CreateThingOnMinioAction`
- [ ] Maybe handle all action types in one process (with threads) or use
      something more sophisticated (Node Red?)
