# Peer-to-peer sensor network with Kafka, UDP and logical clocks (distributed systems lab 2)

Second lab assignment for **Raspodijeljeni sustavi** (Distributed Systems), graduate study, 2025/26.

A decentralised sensor network with no central server. A coordinator controls the sensors through **Apache Kafka**. The sensors exchange NO₂ readings directly with each other over **UDP**, on a simulated unreliable network that loses and delays packets. Each sensor orders the readings it has collected with **scalar (Lamport-style)** and **vector clocks**.

```
                 ┌──────────── Kafka ────────────┐
 Coordinator ──► │  topic "Command": Start/Stop  │ ──► all sensors
                 │  topic "Register": id, port   │ ◄─► all sensors
                 └───────────────────────────────┘

   Sensor A ◄─────── UDP (data + ack) ───────► Sensor B
       ▲                                           ▲
       └──────────── UDP (data + ack) ──► Sensor C ┘
```

## How it works

1. **Control over Kafka.** Each sensor subscribes to the `Command` and `Register` topics and waits. The `Coordinator` publishes `Start`, waits 30 seconds, then publishes `Stop`.
2. **Registration.** On `Start`, each sensor publishes its ID and UDP port to `Register` and records every other sensor that registers.
3. **Readings.** Every 2.5 seconds a sensor reads an NO₂ value from `readings.csv` (row `(seconds active % 100) + 1`) and sends it to every other sensor over UDP.
4. **Unreliable network.** The UDP socket simulates a lossy network: 30% packet loss and an average delay of 1 s.
5. **Reliable delivery.** Every data packet must be acknowledged. If no ACK arrives within 3 seconds, the packet is sent again. Duplicates are detected and answered with an ACK, but not stored twice.
6. **Logical clocks.**
   - *Scalar clock*: each sensor has an emulated system clock with random drift (0–20% per second). On every received message it updates to `max(local, received + 1)`.
   - *Vector clock*: a sensor increments its own entry on each send. On each receive it merges the incoming vector (component-wise max) and increments its own entry.
7. **Ordering and aggregation.** Every 5 seconds each sensor takes the readings from the last 5-second window, prints them sorted by scalar clock and then by vector clock, and prints the window's average NO₂.

On `Stop`, the sensors close their sockets and exit.

## Project layout

```
src/main/java/hr/fer/tel/rassus/stupidudp/
├── sensor/Sensor.java          sensor node: Kafka consumer/producer, UDP peer, clocks, sorting
├── kafka/Coordinator.java      sends Start and, 30 s later, Stop
├── Reading/Reading.java        a reading with its scalar and vector timestamps
├── network/                    simulated lossy UDP socket and emulated drifting clock (from the course template)
├── client/, server/            simple UDP example from the course template
docker-compose.yml              Kafka + ZooKeeper
readings.csv                    sensor data
```

## Running

Requires **Java 11 or newer** and **Docker**.

**1. Start Kafka**

```bash
docker compose up -d
```

Kafka listens on `localhost:9092`.

**2. Build**

```bash
./gradlew jar              # Windows: .\gradlew.bat jar
```

**3. Start the sensors**

Start two or more sensors, each in its own terminal and from the project root (so `readings.csv` is found). Each one asks for an ID and a UDP port:

```bash
java -cp build/libs/2lab-template-1.0-SNAPSHOT.jar hr.fer.tel.rassus.stupidudp.sensor.Sensor
# Enter sensor ID: 1
# Enter sensor port: 5001
```

**4. Start the coordinator**

```bash
java -jar build/libs/2lab-template-1.0-SNAPSHOT.jar
```

The sensors register, exchange readings for 30 seconds, print the sorted windows and averages, and stop.

> Sensors read the `Command` topic from the beginning. Before running the system again, reset Kafka with `docker compose down` and `docker compose up -d`, so new sensors don't pick up the old `Start` and `Stop` commands.

## Tech

Java · Apache Kafka 3.8 (clients) · Docker Compose (Confluent Kafka + ZooKeeper) · UDP · org.json · Gradle
