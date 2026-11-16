# Konfiguration

Verteilte Räume benötigen Redis (`redis.*`), wie jeder Multi-Node-Betrieb von LiveKit.

```yaml
distributed:
  # Feature einschalten, Default: false (Upstream-Verhalten)
  enabled: true

  # Teilnehmer EINES Raums pro Node, bevor weitere Teilnehmer dieses Raums
  # auf einen anderen Node überlaufen. 0 = nur Node-Limits (siehe unten).
  max_participants_per_node: 200

  # Node gilt für neue Teilnehmer als voll, wenn seine CPU-Last (0..1) darüber
  # liegt. 0 = aus.
  cpu_load_limit: 0.8

  # Prüfintervall für ausgefallene Nodes eines Raums. Default: 2s
  node_check_interval: 2s

  relay:
    # UDP-Portbereich der Relay-Verbindungen zu anderen Nodes. Jede Relay-Verbindung
    # (pro Richtung, Raum und anderem Node) belegt einen eigenen Port.
    # Default: 7890-8889. (udp_port ist veraltet und wird als port_range_start verwendet.)
    port_range_start: 7890
    port_range_end: 8889
    # IP, unter der andere Nodes diesen Node erreichen.
    # Reihenfolge: dieser Wert, dann Umgebungsvariable LIVEKIT_RELAY_NODE_IP,
    # dann die Node-IP (rtc.node_ip bzw. automatisch ermittelt).
    node_ip: 10.0.0.11
```

Die beim Start verwendete Relay-IP und ihre Quelle stehen im Log:
`relay node ip {"ip": "10.42.1.17", "source": "LIVEKIT_RELAY_NODE_IP"}`.

**Größe des Relay-Portbereichs:** Jeder Node braucht pro verteiltem Raum und pro anderem
Node dieses Raums bis zu zwei Ports (je einen pro Richtung). Beispiel: 50 Räume, die
jeweils über 5 Nodes verteilt sind, brauchen pro Node bis zu 50 × 4 × 2 = 400 Ports. Ist
der Bereich erschöpft, kommen weitere Relay-Verbindungen nicht zustande
(`relay connection to remote node failed`).

Ein Node gilt für neue Teilnehmer eines Raums als **voll**, wenn eine dieser Bedingungen
zutrifft:

- `max_participants_per_node` ist erreicht. Gezählt werden die Teilnehmer **dieses Raums**
  auf dem Node.
- Ein Node-Limit aus `limit.*` ist erreicht (`num_tracks`, `bytes_per_sec`).
- `cpu_load_limit` ist überschritten.

Neue Nodes für einen Raum wählt der konfigurierte `node_selector` (`kind`, `sort_by`,
`algorithm`) aus, wie bei Upstream.

## Netzwerk

| Port | Richtung | Zweck |
|---|---|---|
| `port` (7880) | Clients → Node | Signalisierung, API |
| `rtc.tcp_port`/`rtc.udp_port` bzw. Port-Range | Clients → Node | Medien zu Clients (unverändert) |
| `distributed.relay.port_range_start`–`port_range_end` (7890–8889/udp) | Node ↔ Node | Medien-Relay (WebRTC, DTLS/SRTP) |
| Redis (6379) | Node → Redis | Zustand, Signal-Relay, Pub/Sub |

- `relay.node_ip` muss von allen anderen Nodes erreichbar sein. Hinter NAT oder in
  Containern ist das typischerweise die interne IP, nicht die öffentliche `rtc.node_ip`.
- Die Relay-Verbindung nutzt nur UDP/IPv4 und ICE-Host-Kandidaten, es gibt kein TURN.
  Die Nodes brauchen also direkte UDP-Erreichbarkeit untereinander.
- Clients können sich mit **jedem** Node verbinden, z. B. über einen Load Balancer. Der
  Teilnehmer wird unabhängig davon auf dem passenden Node gehostet; die Signalisierung
  wird wie bei Upstream per Redis weitergeleitet.

## Kubernetes

Alle Pods verwenden dieselbe Konfiguration (ohne `distributed.relay.node_ip`). Die
Relay-IP kommt pro Pod über die Downward API:

```yaml
env:
  - name: LIVEKIT_RELAY_NODE_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

Für den Relay ist kein `containerPort` und kein Service nötig. Die Nodes verbinden sich
direkt über die Pod-IPs.

- Die Pods müssen sich gegenseitig auf dem Relay-Portbereich (UDP 7890–8889) erreichen. Bei aktiven
  NetworkPolicies muss dieser Verkehr zwischen den LiveKit-Pods erlaubt sein. Ein
  Service für den Relay-Port ist nicht nötig, die Nodes verbinden sich direkt über die
  Pod-IPs.
- Mit `hostNetwork: true` ist die Pod-IP die IP des Kubernetes-Nodes. Dann darf nur ein
  LiveKit-Pod pro Kubernetes-Node laufen, und der Relay-Portbereich muss auf dem Host frei sein.
- Für die Medien zu den Clients gilt dasselbe wie bei Upstream mit mehreren Nodes: Die
  von einem Pod angekündigten ICE-Kandidaten (`rtc.node_ip` bzw. die externe IP plus
  `rtc.tcp_port`/UDP-Ports) müssen **genau diesen Pod** erreichen. Ein Load Balancer, der
  den RTC-Port auf beliebige Pods verteilt, funktioniert nicht, weil der Teilnehmer nur
  auf einem bestimmten Pod gehostet wird. Für die Signalisierung (HTTP/WebSocket) ist ein
  Load Balancer dagegen unproblematisch.

## Hinweise

- Alle Nodes eines Clusters sollten dieselbe `distributed`-Konfiguration verwenden.
- `room.max_participants` gilt weiterhin für den gesamten Raum und zählt Teilnehmer
  auf allen Nodes.
- `room.departure_timeout`/`empty_timeout` gelten pro Node: Ein Node schließt seinen Teil
  des Raums, wenn er keine lokalen Teilnehmer mehr hat. Der Raum endet erst, wenn der
  letzte Node ihn schließt.
