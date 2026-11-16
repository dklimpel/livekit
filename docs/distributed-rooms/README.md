# Verteilte Räume (Distributed Rooms)

Upstream-LiveKit hostet einen Raum immer auf genau **einem** Node. Die maximale
Raumgröße ist dadurch durch die Hardware einer Maschine begrenzt.

Diese Erweiterung verteilt einen Raum auf **mehrere Nodes**. Jeder Node hostet einen
Teil der Teilnehmer. Teilnehmer und Tracks der anderen Nodes werden lokal
gespiegelt, Medien werden bei Bedarf über eine WebRTC-Verbindung zwischen den Nodes
weitergeleitet (Relay). Für Clients bzw. SDKs ändert sich nichts.

Die Erweiterung wird als **Patch-Serie** auf ein Upstream-Release-Tag angewendet, ein
Fork muss nicht gepflegt werden. Ohne `distributed.enabled: true` verhält sich der
Server exakt wie Upstream.

| Dokument | Inhalt |
|---|---|
| [architektur.md](architektur.md) | Aufbau, Datenflüsse, Komponenten, Redis-Schlüssel |
| [konfiguration.md](konfiguration.md) | Konfigurationsoptionen, Ports, Netzwerk |
| [betrieb.md](betrieb.md) | Platzierung, Ausfallverhalten, Monitoring, Einschränkungen |
| [patch-workflow.md](patch-workflow.md) | Patch anwenden, auf neue Upstream-Releases heben, pflegen |
| [tests.md](tests.md) | Unit-Tests, Docker-Compose-Testsetup, lk CLI, Browser-Test |

## Funktionsumfang

| Funktion | Status |
|---|---|
| Teilnehmer eines Raums auf mehreren Nodes (Auffüllen, dann Überlaufen) | ✅ |
| Teilnehmerliste, Metadaten, Attribute, Berechtigungen nodeübergreifend | ✅ |
| Audio/Video-Relay zwischen Nodes, nur bei Bedarf | ✅ |
| Simulcast (alle Layer werden weitergeleitet, Layer-Auswahl pro Node) | ✅ |
| Dynacast (Qualitätswünsche entfernter Nodes erreichen den Publisher) | ✅ |
| NACK, PLI/Keyframe-Anforderung, Sender Reports (A/V-Sync) über das Relay | ✅ |
| Data Packets inkl. RPC, Chat, Text-/Byte-Streams | ✅ |
| Active Speaker nodeübergreifend | ✅ |
| Raum-Metadaten, Raum löschen (API) auf allen Nodes | ✅ |
| Node-Ausfall: Raum läuft weiter, Teilnehmer verbinden sich neu | ✅ |
| RoomService-API (List/Get/Update/Remove/Mute/SendData) | ✅ |
| Webhooks `room_started`/`room_finished` nur einmal pro Raum | ✅ |
| Egress, Agents, SIP im verteilten Raum | ⚠️ siehe [betrieb.md](betrieb.md#einschränkungen) |
| Data Tracks, unlabeled Data Messages, Connection-Quality entfernter Teilnehmer | ❌ noch nicht |

## Schnellstart

```bash
# Patches auf ein Upstream-Release anwenden und bauen
patches/distributed-rooms/apply.sh v1.13.11 /tmp/livekit-dist

# lokales Test-Cluster (Redis + 3 Nodes) und alle Szenarien
cd /tmp/livekit-dist/test/distributed && ./run-tests.sh
```

Minimale Konfiguration (zusätzlich zu Redis):

```yaml
distributed:
  enabled: true
  max_participants_per_node: 200
```
