# Scripts

A small set of helper scripts is provided to simplify common development and demo workflows. They are optional convenience wrappers around **Docker Compose**, not a replacement for it.

For advanced or fine-grained control, you are encouraged to interact with Docker Compose directly.

### build.sh
Builds all Docker images defined in docker-compose.yml. It builds, in order, the base image and then all the services images. It is recommended for a first-time setup. 

#### Usage:
```
bash build.sh
```

### run.sh
Starts the essential pipeline services required for data ingestion and storage:
- Kafka + topic initialization
- MinIO + bucket initialization
- Producer
- Consumer

#### Usage:
```
bash run.sh
```

You can optionally scale producers and consumers. If no parameters are provided, the default scale value is 1.

```
bash run.sh producer=3 consumer=4
```

### run_all.sh
Starts all pipeline services, including optional and exploratory components:
- All services started by run.sh
- Kafka UI
- Querier backend
- Streamlit SQL editor

#### Usage:
```
bash run_all.sh
```

Optional scaling is supported as well:

```
bash run_all.sh producer=3 consumer=4
```

### shutdown.sh
Stops and cleanly removes all containers launched for the running pipeline.

#### Usage:
```
bash shutdown.sh
```

## Advanced usage
These scripts intentionally cover only the most common workflows.

For fine-grained control (running a single service, inspecting logs, restarting components, etc.), use Docker Compose directly.

Some examples:
```
docker compose up producer
docker compose up -d --scale consumer=3
docker compose logs -f consumer
docker compose restart querier
```