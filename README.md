# Reactor Egress Worker

RTMP egress worker for Reactor video streams.

## CLI

Run from YAML/JSON config:

```bash
reactor-egress-worker run --config config.yaml
```

## SDK (embedded usage)

Use the worker from your own Python process without launching a separate isolate deployment:

```python
from worker import EgressWorker

worker = EgressWorker.from_file("config.yaml")
exit_code = await worker.run()
```

If your host app already has a Reactor client instance, inject it directly:

```python
from worker import EgressWorker

worker = EgressWorker.from_file("config.yaml", reactor_client=reactor_client)
exit_code = await worker.run()
```

You can also pass a validated `WorkerConfig` or plain `dict`:

```python
from worker import run_egress

exit_code = await run_egress(config_dict)
```

For synchronous callers:

```python
from worker import run_egress_sync

exit_code = run_egress_sync("config.yaml")
```
