# Architektur

## Überblick

```
                 ┌──────────────── Redis ────────────────┐
                 │ Node-Registry, Raum→Nodes,             │
                 │ Teilnehmer→Node, Pub/Sub-Kanäle        │
                 └───────▲───────────────▲───────────────┘
                         │               │
        ┌────────────────┴──┐         ┌──┴────────────────┐
        │ Node A            │  WebRTC │ Node B            │
        │ rtc.Room "demo"   │◄───────►│ rtc.Room "demo"   │
        │  alice (lokal)    │  Relay  │  bob (lokal)      │
        │  bob   (remote)   │         │  alice (remote)   │
        └───────▲───────────┘         └───────▲───────────┘
                │                             │
             Client alice                  Client bob
```

Jeder Node, der Teilnehmer eines Raums hostet, betreibt eine normale `rtc.Room`-Instanz
mit seinen **lokalen** Teilnehmern. Teilnehmer der anderen Nodes werden als
`RemoteParticipant` in diesen Raum gespiegelt. Für lokale Teilnehmer sehen sie aus wie
normale Teilnehmer: Sie erscheinen im Join-Response, in Participant-Updates, Active
Speakers usw.

Tracks entfernter Teilnehmer sind normale `rtc.MediaTrack`-Objekte (`remoteTrack`).
Statt von einem Client werden ihre Receiver vom Relay gespeist. Dadurch funktionieren
Downtracks, Simulcast-Layer-Auswahl, Dynacast, Pacing und Congestion Control zu den
Subscribern unverändert.

## Komponenten (`pkg/distributed`)

| Datei | Aufgabe |
|---|---|
| `manager.go` | Einstiegspunkt pro Node. Wird vom `RoomManager` erzeugt, verwaltet die verteilten Räume des Nodes, empfängt Relay-Nachrichten. Alle Methoden sind auf `nil` sicher (Feature aus). |
| `placement.go` | Entscheidet, auf welchem Node ein Teilnehmer gehostet wird („Auffüllen, dann Überlaufen“). |
| `store.go` | Cluster-Zustand in Redis (Raum→Nodes, Teilnehmer→Node, Locks). |
| `roomstate.go` | Verteilter Teil eines Raums auf einem Node: Abgleich der Teilnehmer, Data Packets, Sprecher, Metadaten, Raum schließen. |
| `remoteparticipant.go` | Spiegel eines Teilnehmers eines anderen Nodes (`types.Participant`). |
| `remotetrack.go` | Track eines anderen Nodes, fordert das Relay bei der ersten Subscription an. |
| `relayconn.go` | Relay zu einem Peer-Node für einen Raum, empfangende Seite. |
| `relaysender.go` | Relay zu einem Peer-Node für einen Raum, sendende Seite, inkl. `relayDownTrack`. |
| `relaytransport.go` | Gemeinsamer UDP-Port und PeerConnection-Fabrik für Relays. |
| `nodecheck.go` | Erkennt ausgefallene Nodes eines Raums. |
| `messages.go` | Nachrichtenformat zwischen den Nodes. |

Einhängepunkte in Upstream-Code (bewusst klein gehalten, siehe [patch-workflow.md](patch-workflow.md)):

| Datei | Änderung |
|---|---|
| `pkg/config/config.go` | Feld `Distributed` (Typ in neuer Datei `config/distributed.go`) |
| `pkg/routing/redisrouter.go` | optionaler `ParticipantPlacer` (neue Datei `routing/distributed_hook.go`) |
| `pkg/service/signal.go` | Session-Annahme ohne „Raum gehört diesem Node“-Prüfung |
| `pkg/service/roomallocator.go` | kein Abweisen bei vollem Node (Überlauf statt Fehler) |
| `pkg/service/roommanager.go` | Manager erzeugen, Raum an-/abmelden, Telemetrie einmal pro Raum, Teilnehmerliste inkl. Remote |
| `pkg/rtc/room.go` | Remote-Teilnehmer in Join-Response, Auto-Subscribe, Berechtigungen, Sprecher, Zähler; Data-Packets/Berechtigungen nach außen melden (Logik in neuer Datei `rtc/room_distributed.go`) |

## Ablauf: Teilnehmer verbindet sich

1. Der Client verbindet sich mit einem beliebigen Node (Signal-Node), z. B. über einen Load Balancer.
2. `RTCService` → `RoomAllocator.SelectRoomNode`. Im verteilten Modus wird der Raum nur
   dann abgewiesen, wenn **alle** Nodes voll sind.
3. `RedisRouter.StartParticipantSignal` fragt den `Placer` nach dem Node für **diesen
   Teilnehmer**:
   - Ist die Identität bereits einem lebenden Node zugeordnet (Resume, doppelte
     Identität), bleibt sie dort.
   - Sonst wird der vollste Node des Raums unterhalb seiner Limits gewählt (Auffüllen).
   - Sind alle Nodes des Raums voll, wählt der konfigurierte Node-Selector einen
     weiteren Node aus (Überlaufen).
   - Die Zuordnung wird in `dist:participant_node:<raum>` gespeichert. Ein Redis-Lock pro
     Raum serialisiert gleichzeitige Platzierungen.
4. Die Signalisierung wird wie bei Upstream per psrpc an den gewählten Node weitergeleitet.
5. Erzeugt der Node den Raum lokal zum ersten Mal, meldet er sich in `dist:room_nodes:<raum>`
   an, abonniert den Raum-Kanal und fordert per `sync_request` den Zustand der anderen Nodes an.

## Ablauf: Zustandsabgleich

Jeder Node veröffentlicht Änderungen seiner lokalen Teilnehmer auf dem Kanal
`dist|room|<raum>`:

| Nachricht | Auslöser | Wirkung auf den anderen Nodes |
|---|---|---|
| `participant_update` | jede Teilnehmeränderung (Join, Tracks, Mute, Metadaten, Attribute, Berechtigungen, Leave) | Remote-Teilnehmer anlegen/aktualisieren/entfernen, Tracks veröffentlichen/entfernen |
| `sync_request` | Node tritt dem Raum bei | alle lokalen Teilnehmer erneut senden |
| `data_packet` | Data Packet eines lokalen Teilnehmers oder der API | an lokale Teilnehmer zustellen (Ziel-Identitäten werden lokal gefiltert) |
| `speakers` | periodisch (`audio.update_interval`), solange gesprochen wird | Audiopegel der Remote-Teilnehmer, die Sprechererkennung läuft lokal |
| `room_metadata` | Raum-Metadaten geändert | Metadaten übernehmen |
| `room_close` | Raum per API gelöscht | Raum lokal schließen |
| `node_left` | lokaler Raum geschlossen (leer, Shutdown) | Teilnehmer dieses Nodes entfernen |

Die Nachrichten sind JSON-Umschläge (`wrapperspb.BytesValue`) über den vorhandenen
psrpc-Redis-Bus. LiveKit-Protos darin sind binär kodiert. Eine Änderung an
`github.com/livekit/protocol` ist dafür nicht nötig.

## Ablauf: Medien-Relay

```
Node A (Publisher alice)                         Node B (Subscriber bob)
                                                 bob abonniert alices Track
                                                 → remoteTrack.AddSubscriber
                         ◄── relay_subscribe ──
relaySender.addTrack:
 pro Codec × Simulcast-Layer ein
 sendonly-Transceiver + relayDownTrack
 am lokalen Receiver
                         ── relay_offer ──────►
                         ◄── relay_answer ─────   relayConn.handleOffer
                         ◄─── ICE / DTLS ─────►
RTP aller Layer ──────────────────────────────►  OnTrack → MediaTrack.AddReceiver
                                                 (Layer über RID zugeordnet)
                                                 → normale Downtracks zu bob
                         ◄── RTCP: NACK, PLI ──   (vom LiveKit-Buffer erzeugt)
NACK: pion NACK-Responder
PLI : Receiver.SendPLI → Publisher
Sender Reports des Publishers ────────────────►  A/V-Sync bleibt erhalten
                         ◄── relay_quality ────   Dynacast von B
NotifySubscriberNodeMaxQuality(B, …)
```

- Pro Raum und Node-Paar gibt es höchstens eine empfangende und eine sendende
  PeerConnection. Die sendende Seite ist immer der Offerer.
- Ein Track wird **einmal pro Node** übertragen, egal wie viele Subscriber dort sind.
- Alle Simulcast-Layer werden weitergeleitet, die Layer-Auswahl pro Subscriber erfolgt
  auf dem empfangenden Node. Nicht benötigte Layer pausiert der Publisher über Dynacast,
  sobald kein Node sie mehr anfordert.
- Header-Extension-IDs werden auf die IDs der Relay-Verbindung umgeschrieben.
- Jede Relay-Verbindung belegt einen eigenen UDP-Port aus dem Relay-Portbereich
  (`distributed.relay.port_range_start`–`port_range_end`) und ist DTLS/SRTP-verschlüsselt.
  Ein gemeinsamer Port für alle Verbindungen funktioniert nicht: Die Verbindungen beider
  Richtungen zwischen denselben zwei Nodes hätten dasselbe Adresspaar und würden sich
  gegenseitig stören.
- Die WebRTC-Logs (pion) des Relays laufen unter der Komponente `transport` und folgen
  damit `logging.pion_level`, wie die Verbindungen zu den Clients.

## Redis-Schlüssel

| Schlüssel | Typ | Inhalt |
|---|---|---|
| `dist:room_nodes:<raum>` | Hash | Node-ID → Unix-Zeit des Beitritts |
| `dist:participant_node:<raum>` | Hash | Identität → Node-ID |
| `dist:room_meta:<raum>` | Hash | `agents_node`: Node, der Raum-Agents dispatcht |
| `dist:placement_lock:<raum>` | String | kurzlebiger Platzierungs-Lock |
| `dist:node_relay` | Hash | Node-ID → Relay-Adresse `ip:port`. Jeder Node erneuert seinen Eintrag alle 30 s und entfernt Einträge nicht verfügbarer Nodes. |
| `room_node_map` (Upstream) | Hash | Raum → ein lebender Node des Raums (Kompatibilität) |

Pub/Sub-Kanäle: `dist|room|<raum>` (Zustand), `dist|node|<node-id>` (Relay-Steuerung).
