# Betrieb

## Platzierung „Auffüllen, dann Überlaufen“

Neue Teilnehmer landen bevorzugt auf dem **vollsten** Node, der den Raum bereits hostet
und noch nicht voll ist. Erst wenn alle Nodes des Raums voll sind, kommt ein weiterer
Node hinzu. So bleibt der Relay-Verkehr gering: Kleine Räume bleiben auf einem Node, und
große Räume verteilen sich auf möglichst wenige Nodes.

Ein Teilnehmer, dessen Identität bereits einem lebenden Node zugeordnet ist, wird immer
dorthin geleitet. Das betrifft Resume, Reconnect und den Ersatz einer doppelten Identität.

Die Platzierung wird geloggt:

```
INFO livekit.distributed placed participant {"room": "demo", "participant": "bob",
     "nodeID": "ND_…", "reason": "overflow", "roomNodes": ["ND_…"], "roomParticipantsOnNode": 1}
```

`reason`: `new` (erster Node des Raums), `fill`, `overflow`, `assigned` (bestehende Zuordnung).

## Ausfall eines Nodes

1. Ein Node meldet seine Statistiken alle paar Sekunden in Redis. Sind sie älter als
   5 s, gilt er als nicht verfügbar. Das ist der Upstream-Mechanismus (`selector.AvailableSeconds`).
2. Die anderen Nodes des Raums prüfen das alle `node_check_interval`. Bei einem
   ausgefallenen Node entfernen sie seine Teilnehmer und Tracks lokal. Die Clients sehen
   die Teilnehmer gehen, Subscriptions auf deren Tracks enden. Außerdem löschen sie ihn
   aus `dist:room_nodes`, aus den Teilnehmer-Zuordnungen und aus dem Room-Store
   (für `ListParticipants`).
3. Die Clients des ausgefallenen Nodes verlieren ihre Verbindung. Das SDK versucht
   zuerst ein Resume. Die Platzierung ignoriert Zuordnungen zu toten Nodes, der
   Resume-Versuch landet also auf einem lebenden Node, der die Session nicht kennt. Er
   antwortet mit `Leave(RECONNECT)`, und das SDK verbindet sich vollständig neu.
4. Der Raum selbst läuft auf den übrigen Nodes ohne Unterbrechung weiter.
5. Schließt ein Node seinen Teil des Raums, entfernt er dabei auch nicht mehr
   verfügbare Nodes aus `dist:room_nodes`. So hält ein abgestürzter oder mit neuer
   Node-ID neu gestarteter Node einen Raum nicht dauerhaft am Leben. Welcher Node der
   letzte ist und den Raum beendet (Room-Store, `room_finished`), entscheidet Redis
   atomar, auch wenn mehrere Nodes gleichzeitig schließen.

Typische Erkennungszeit: 5 bis 8 s. Im Testlauf wurde ein beendeter Node nach etwa 4 s
entfernt.

> Wichtig: Fällt ein Node aus, über den ein Client nur **signalisiert** (Signal-Node ≠
> RTC-Node), verliert dieser Client ebenfalls seine Verbindung und verbindet sich neu.
> Das ist Upstream-Verhalten des Signal-Relays.

Geordnetes Herunterfahren (`SIGTERM`/Drain): Der Node nimmt keine neuen Teilnehmer an
(Status `SHUTTING_DOWN`), schließt seine Räume und meldet `node_left`. Die anderen Nodes
entfernen seine Teilnehmer sofort.

## Monitoring

Relevante Logmeldungen (Komponente `livekit.distributed` bzw. `livekit.room.distributed`):

| Meldung | Bedeutung |
|---|---|
| `distributed rooms enabled` | Feature aktiv (beim Start) |
| `relay transport` | Relay-Portbereich und IP |
| `placed participant` | Platzierungsentscheidung |
| `room attached to distributed room` | Node hostet nun einen Teil des Raums (`numNodes`) |
| `remote participant joined/left` | Spiegelung von Teilnehmern anderer Nodes |
| `relaying track to remote node` / `stopped relaying track` | sendende Seite des Relays |
| `relayed track received` (debug) | empfangene Relay-Streams |
| `remote node of room is unavailable` | Node-Ausfall erkannt |
| `relay node ip` | beim Start: verwendete Relay-IP und Quelle (`config`, `LIVEKIT_RELAY_NODE_IP`, `node_ip`) |
| `relay connection to remote node failed` | Relay-Verbindung kam nicht zustande, enthält beide Relay-Adressen. Fast immer ist der Relay-UDP-Portbereich zwischen den Nodes nicht erreichbar oder die Relay-IP falsch. Neuer Versuch mit wachsender Pause (3 s bis 60 s, `retrying sending relay connection later`). |

Den Zustand eines Raums in Redis prüfen:

```bash
redis-cli HGETALL dist:room_nodes:demo
redis-cli HGETALL dist:participant_node:demo
redis-cli HGETALL dist:node_relay
```

## Einschränkungen

| Bereich | Verhalten |
|---|---|
| Agents | Raum-Agents dispatcht genau ein Node pro Raum. Teilnehmer- und Publisher-Jobs dispatcht der Node des jeweiligen Teilnehmers. Ein Agent ist ein normaler Teilnehmer und kann überall platziert werden. Nicht systematisch getestet. |
| Egress | Track-/Participant-Egress startet der Node des Publishers. Room-Composite-Egress tritt als Teilnehmer bei und sieht alle Tracks über das Relay. Nicht systematisch getestet. |
| SIP, Ingress, WHIP | Teilnehmer werden normal platziert. Nicht getestet. |
| Relay-Lebensdauer | Ein Track bleibt zu einem Node weitergeleitet, bis er unveröffentlicht wird oder der Node den Raum verlässt, auch wenn dort niemand mehr abonniert. Dynacast pausiert dann aber die Video-Layer, sofern sonst niemand sie braucht. |
| Backup-Codecs | Es werden die Codecs weitergeleitet, die zum Zeitpunkt der ersten Subscription empfangen werden. |
| Relay-Verbindungsabbruch | Bricht eine Relay-Verbindung ab (ohne Node-Ausfall), wird sie bei der nächsten Anforderung neu aufgebaut. Bereits weitergeleitete Tracks erholen sich dabei nicht in jedem Fall. |
| Data Tracks, unlabeled Data Messages | Werden nicht zwischen Nodes weitergeleitet. |
| Connection Quality | Die Verbindungsqualität entfernter Teilnehmer wird nicht übertragen. |
| Topologie | Voller Mesh zwischen den Nodes eines Raums. Für 5 bis 20 Nodes pro Raum ausgelegt. Bei viel mehr Nodes wäre ein Fan-out-Baum sinnvoll. |
| Congestion Control Node↔Node | Keine Bandbreitenschätzung auf der Relay-Strecke. Es wird ein gut angebundenes Rechenzentrumsnetz vorausgesetzt. |
