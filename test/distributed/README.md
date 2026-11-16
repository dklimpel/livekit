# Distributed rooms – local test cluster

Redis + 3 LiveKit nodes with distributed rooms enabled, built from this source tree.

```bash
docker compose up -d --build
./run-tests.sh                 # go, loadtest, failover scenarios; exit code 0 = pass
```

Browser test page: http://localhost:8090. Documentation (German):
[docs/distributed-rooms/tests.md](../../docs/distributed-rooms/tests.md).
