# Patch-Workflow

Die Erweiterung wird nicht als Fork gepflegt, sondern als Patch-Serie gegen ein
Upstream-**Release-Tag**. Die Patches liegen unter `patches/distributed-rooms/`:

| Patch | Inhalt | Konfliktrisiko |
|---|---|---|
| `0001-distributed-rooms-upstream-hooks.patch` | Einhängepunkte in 6 Upstream-Dateien (ca. 80 geänderte Zeilen) und 4 neue Dateien in Upstream-Paketen | gering bis mittel, einziger Patch, der Upstream-Code ändert |
| `0002-distributed-rooms-implementation.patch` | neues Paket `pkg/distributed` | keins (neue Dateien), ggf. API-Anpassungen |
| `0003-distributed-rooms-tests.patch` | `test/distributed`: E2E-Tests, Docker Compose, Skripte, Testseite | keins |
| `0004-distributed-rooms-docs.patch` | diese Dokumentation | keins |

`BASE` enthält das Release-Tag, gegen das die Patches erzeugt wurden.

## Patches anwenden

```bash
patches/distributed-rooms/apply.sh v1.13.11 ./livekit-dist
```

Das Skript klont `github.com/livekit/livekit` (oder nutzt einen vorhandenen Checkout im
Zielverzeichnis), legt den Branch `distributed-<tag>` an, wendet die Patches mit
3-Way-Merge an, baut und führt die Unit-Tests aus. Die Änderungen werden nicht
committet, damit sie vor dem Commit noch geprüft werden können.

Ohne Skript:

```bash
git checkout v1.13.11
git apply --3way patches/distributed-rooms/0*.patch
go build ./...
```

Docker-Image bauen, wie bei Upstream:

```bash
docker build -t livekit-server:v1.13.11-distributed .
```

## Auf ein neues Upstream-Release heben

1. Patches auf das neue Tag anwenden:
   ```bash
   patches/distributed-rooms/apply.sh v1.14.0 ./livekit-v1.14.0
   ```
2. Bei einem Konflikt in Patch 0001 die markierten Stellen auflösen (`git status`,
   `git diff`). Jeder Einhängepunkt ist eine kleine, in sich geschlossene Änderung. Die
   Tabelle unten beschreibt, was er tut, sodass er sich auch in umgebautem Upstream-Code
   wieder einsetzen lässt. Danach die übrigen Patches anwenden:
   ```bash
   git apply --3way ../patches/distributed-rooms/000{2,3,4}-*.patch
   ```
3. Bauen, Unit-Tests und das Docker-Testsetup ausführen:
   ```bash
   go build ./... && go test ./pkg/distributed/ ./pkg/rtc/ ./pkg/routing/...
   cd test/distributed && ./run-tests.sh
   ```
4. Patches für das neue Release neu erzeugen und die Patch-Ablage aktualisieren:
   ```bash
   patches/distributed-rooms/make-patches.sh v1.14.0
   ```

Kompilierfehler in `pkg/distributed` nach einem Upstream-Update entstehen typischerweise
durch geänderte interne Schnittstellen (`types.Participant`, `types.MediaTrack`,
`sfu.TrackSender`, `rtc.MediaTrackParams`). Der Compiler zeigt die fehlenden oder
geänderten Methoden direkt an.

## Einhängepunkte im Upstream-Code

| Datei | Stelle | Zweck |
|---|---|---|
| `pkg/config/config.go` | `Config` | Feld `Distributed DistributedConfig` |
| `pkg/routing/redisrouter.go` | Struct `RedisRouter` | Feld `placer ParticipantPlacer` |
| | `GetNodeForRoom` | mit Placer: Node für den nächsten Teilnehmer statt fester Raum-Node |
| | `StartParticipantSignal` | mit Placer: Node pro Teilnehmer |
| `pkg/service/signal.go` | `HandleSession` | mit Placer: Session ohne „falscher RTC-Node“-Prüfung annehmen |
| `pkg/service/roomallocator.go` | `SelectRoomNode` | Node-Limits führen im verteilten Modus nicht zur Ablehnung |
| `pkg/service/roommanager.go` | Struct, `NewLocalRoomManager`, `Stop` | `distributed.Manager` erzeugen bzw. stoppen, Aufräum-Callback für ausgefallene Nodes |
| | `getOrCreateRoom` | `PrepareRoomInternal` (Raum-Agents nur einmal), `AttachRoom`, `DetachRoom`, Telemetrie `RoomStarted`/`RoomEnded` nur auf erstem/letztem Node, Store nur vom letzten Node löschen |
| | `OnParticipantChanged`, `OnRoomUpdated` | Änderungen an `distributed` melden; Remote-Teilnehmer nicht in den Room-Store schreiben |
| | `ListParticipants`, `GetParticipant` | Remote-Teilnehmer einbeziehen |
| `pkg/rtc/room.go` | Struct `Room` | Feld `distributed roomDistributedState` |
| | `GetActiveSpeakers` | inkl. Remote-Teilnehmer (aufgeteilt in `getActiveSpeakers`) |
| | `Join` | `max_participants` zählt Remote-Teilnehmer mit |
| | `ResolveMediaTrack/DataTrackForSubscriber` | Berechtigung auch bei Remote-Publishern prüfen |
| | `onUpdateSubscriptionPermission` | Berechtigungsänderung an andere Nodes melden |
| | `createJoinResponseLocked` | Remote-Teilnehmer in `OtherParticipants` |
| | `onTrackPublished` | keine Agents/Egress für Remote-Teilnehmer |
| | `onDataMessage` | Data Packets an andere Nodes melden |
| | `subscribeToExistingTracks` | Auto-Subscribe auch auf Remote-Tracks |
| | `updateProto` | Teilnehmer-/Publisher-Zähler inkl. Remote |

Die eigentliche Logik der Einhängepunkte steckt in neuen Dateien der jeweiligen Pakete
(`*_distributed.go`, `distributed_hook.go`). Diese Dateien erzeugen keine Konflikte.

## Regeln für Änderungen an der Erweiterung

- Neue Logik in `pkg/distributed` oder in neue `*_distributed.go`-Dateien legen.
- Upstream-Dateien nur mit kurzen Aufrufen erweitern und keinen Code verschieben,
  umbenennen oder umformatieren.
- Keine Änderungen an `github.com/livekit/protocol` und keine neuen Abhängigkeiten in
  `go.mod`.
- Nach Änderungen `make-patches.sh <base-tag>` ausführen und die Patches einchecken.
