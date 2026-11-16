# Tests

## Unit-Tests

```bash
go test ./pkg/distributed/
```

Die Unit-Tests decken die Platzierung (Auffüllen, Überlaufen, alles voll, toter Node,
CPU-Limit), Remote-Teilnehmer (Versionen, Berechtigungen, Sprecher-Timeout, Disconnect),
die Kodierung der Relay-Stream-IDs und das Nachrichtenformat ab.

Die bestehenden Upstream-Tests der geänderten Pakete laufen unverändert:

```bash
go test ./pkg/rtc/ ./pkg/routing/... ./pkg/config/ ./pkg/service/   # pkg/service braucht Docker (Redis)
```

## Lokales Test-Cluster mit Docker Compose

`test/distributed/` enthält ein Cluster aus Redis und 3 LiveKit-Nodes, gebaut aus dem
lokalen Quellcode:

```bash
cd test/distributed
docker compose up -d --build          # Cluster starten
./run-tests.sh                        # alle Szenarien, Exit-Code 0/1
./run-tests.sh loadtest failover      # einzelne Szenarien
KEEP=1 ./run-tests.sh loadtest        # Cluster danach nicht herunterfahren
docker compose down
```

| Node | Signal/API | RTC TCP | RTC UDP | Relay (intern, UDP 7890–7989) |
|---|---|---|---|---|
| livekit-1 | ws://localhost:7880 | 7881 | 7882 | 172.28.0.11:7890 |
| livekit-2 | ws://localhost:7980 | 7981 | 7982 | 172.28.0.12:7890 |
| livekit-3 | ws://localhost:8080 | 8081 | 8082 | 172.28.0.13:7890 |

API-Key `devkey`, Secret `secretsecretsecretsecretsecretsecret`.
`MAX_PER_NODE` (Default 1) setzt `distributed.max_participants_per_node`, `LOG_LEVEL`
die Log-Stufe, z. B. `MAX_PER_NODE=4 LOG_LEVEL=debug docker compose up -d`.

### Szenarien von `run-tests.sh`

| Szenario | Ablauf | Prüfungen |
|---|---|---|
| `go` | Go-E2E-Tests (`test/distributed/e2e_test.go`) gegen das Cluster mit `MAX_PER_NODE=1`. Braucht Go auf dem Host, sonst übersprungen. | 3 Teilnehmer auf 3 Nodes sehen sich. Audio und Video werden über Nodes empfangen, auch in beide Richtungen zwischen denselben Nodes, Data Packets kommen an, ein Austritt wird überall sichtbar. Node-Ausfall: Teilnehmer wird entfernt (Client und RoomService), Rejoin auf lebendem Node, Medien danach |
| `loadtest` | `lk load-test` mit 3 Video-Publishern (Simulcast), 3 Audio-Publishern und 6 Subscribern über einen Node, `MAX_PER_NODE=4` | Teilnehmer auf ≥ 2 Nodes. Exit-Code von lk ist 0. Alle Tracks abonniert (sofern lk eine Zusammenfassung ausgibt). Tracks werden weitergeleitet, ohne Relay-Fehler |
| `failover` | `lk load-test` läuft, ein Node mit Teilnehmern wird per `SIGKILL` beendet | Node aus dem Raum entfernt, keine Zuordnung mehr zum toten Node, Raum lebt weiter, neuer Teilnehmer (`lk room join`) landet auf lebendem Node |

### lk CLI direkt verwenden

Der Service `lk` (Profil `tools`) nutzt das Host-Netzwerk und ist auf livekit-1 vorkonfiguriert:

```bash
docker compose run --rm lk room list
docker compose run --rm lk room participants list demo
docker compose run --rm lk load-test --room demo --video-publishers 2 --subscribers 10 --duration 1m
# über einen anderen Signal-Node
docker compose run --rm lk --url ws://localhost:7980 room participants list demo
```

Die Verteilung eines Raums ist in Redis sichtbar:

```bash
docker compose exec redis redis-cli HGETALL dist:participant_node:demo
```

### Manueller Test im Browser

1. Cluster starten. Mit dem Default `MAX_PER_NODE=1` landet jeder Tab auf einem eigenen Node:
   `docker compose up -d --build`
2. Tokens erzeugen (gültig 2 h):
   ```bash
   for id in alice bob carol; do
     docker compose run --rm lk token create --join --room demo --identity $id --valid-for 2h
   done
   ```
3. http://localhost:8090 in mehreren Tabs öffnen, jeweils einen Signal-Node wählen, ein
   Token einfügen und verbinden.
4. Erwartung: Alle Tabs sehen sich gegenseitig mit Video und Audio, die Sprecher-
   Hervorhebung funktioniert, die Verteilung ist in Redis bzw. im Log
   (`placed participant`) sichtbar.
5. Ausfall testen: `docker compose kill livekit-2`. Teilnehmer auf diesem Node
   verschwinden nach einigen Sekunden aus den anderen Tabs, ihr Tab verbindet sich neu.

## Go-E2E-Tests gegen ein beliebiges Cluster

```bash
LIVEKIT_DISTRIBUTED_E2E=1 \
LIVEKIT_NODE_URLS=ws://localhost:7880,ws://localhost:7980,ws://localhost:8080 \
LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secretsecretsecretsecretsecretsecret \
go test ./test/distributed/ -run TestDistributedRoom -v
```

Für `TestNodeFailure` zusätzlich `LIVEKIT_REDIS_ADDR`, `LIVEKIT_NODE_RELAY_ADDRS` (in der
Reihenfolge der URLs) und `LIVEKIT_STOP_NODE_CMD` (z. B. `./test/distributed/stop-node.sh {node}`).
Beide Tests erwarten `max_participants_per_node: 1`. Mit `LIVEKIT_E2E_SIGNALLING=v1` nutzen
die Testclients die Single-PeerConnection-Signalisierung (`/rtc/v1`) aktueller Browser-SDKs
statt der klassischen Signalisierung mit zwei PeerConnections.
