TSM Dispatcher is a service to handle a collection of actions when they are triggered by Apache Kafka events.

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
