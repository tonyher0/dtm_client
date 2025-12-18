# dtm_client

Rust client library for DTM (Distributed Transaction Manager): https://dtm.pub/

This crate targets feature parity with the upstream Go reference client:
- https://github.com/dtm-labs/client (baseline: `main@4be5512`)

## Features

- `http` (default): HTTP client (`dtmcli`) via `reqwest`
- `grpc` (default): gRPC client (`dtmgrpc`) via `tonic`
- `workflow` (default): workflow engine aligned with Go `workflow`
- `barrier-redis` (default): Redis barrier utilities (Redis-only scope)

Disable defaults if you want a smaller dependency footprint:
```bash
cargo add dtm_client --no-default-features --features http
```

## Examples (dtmcli / dtmgrpc / workflow)

DTM quick start projects (recommended to run locally):
- https://github.com/dtm-labs/quick-start-sample
- https://github.com/dtm-labs/dtm-examples

Environment variables used by examples:
- `DTM_HTTP_URL` (default `http://localhost:36789/api/dtmsvr`)
- `DTM_GRPC_ADDR` (default `localhost:36790`)
- `BUSI_HTTP_BASE` (default `http://localhost:8082/api/busi_start`)
- `BUSI_GRPC_ADDR` (default `localhost:50589`)
- `WORKFLOW_HTTP_CALLBACK` (default `http://localhost:8080/workflowResume`)
- `WORKFLOW_GRPC_LISTEN` (default `127.0.0.1:50051`)
- `WORKFLOW_GRPC_CLIENT_HOST` (default `127.0.0.1:50051`)
- `REDIS_URL` (default `redis://127.0.0.1/`)
- `GID` (optional): if set, examples will reuse it instead of calling DTM `/newGid` or `NewGid`.

Run examples:
```bash
cargo run --example dtmcli_saga
cargo run --example dtmgrpc_saga
cargo run --example workflow_http
cargo run --example workflow_grpc
cargo run --example barrier_redis
```

Notes:
- If you run DTM in Docker and your business services are on the host, set `IS_DOCKER=1` so `localhost` is rewritten to `host.docker.internal`.
- gRPC branch URLs use the format `host:port/fully.qualified.Service/Method` (e.g. `localhost:50589/busi.Busi/TransOut`).

## Parity tracking

See `docs/parity.md`.
