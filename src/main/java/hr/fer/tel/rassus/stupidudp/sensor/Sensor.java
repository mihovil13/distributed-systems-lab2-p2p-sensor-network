package hr.fer.tel.rassus.stupidudp.sensor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;
import java.time.Duration;

import hr.fer.tel.rassus.stupidudp.Reading.Reading;
import hr.fer.tel.rassus.stupidudp.network.EmulatedSystemClock;
import hr.fer.tel.rassus.stupidudp.network.SimpleSimulatedDatagramSocket;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

public class Sensor {
    private String csvFilePath = "readings.csv";
    private List<String[]> lines;
    private String sensorId;
    private int sensorPort;
    private long activeSeconds;
    private Map<String, JSONObject> otherSensors = new HashMap<>();
    private SimpleSimulatedDatagramSocket udpSocket;
    private EmulatedSystemClock scalarClock;
    private Map<String, Integer> vectorClock = new HashMap<>();
    private Set<Long> receivedPackets = new HashSet<>();
    private Map<String, Boolean> receivedAcks = new HashMap<>();
    private volatile boolean udpRunning = true;
    private List<Reading> allReadings = new ArrayList<>();

    public Sensor(String sensorId, Integer port) {
        this.lines = new ArrayList<>();
        this.sensorId = sensorId;
        this.sensorPort = port;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line.split(",", -1));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CSV file", e);
        }
    }

    public String getCurrentNO2() {
        long seconds = (System.currentTimeMillis() - activeSeconds) / 1000;
        int rowIndex = (int) (seconds % 100) + 1;
        String[] row = lines.get(rowIndex);
        String value = "0";
        if (!row[4].isEmpty()) value = row[4];
        return value;
    }

    public void start() throws SocketException {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Sensor " + sensorId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        scalarClock = new EmulatedSystemClock();
        udpSocket = new SimpleSimulatedDatagramSocket(sensorPort, 0.3, 1000);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Arrays.asList("Command", "Register"));
            boolean running = false;
            System.out.println("Node waiting for Start command...");

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for (var record : consumerRecords) {
                    if (record.topic().equals("Command")) {
                        String command = record.value();
                        System.out.println("Received command: " + command);

                        if ("Start".equals(command)) {
                            if (!running) {
                                sendRegistration();
                                running = true;
                                System.out.println("Sensor started");
                                this.activeSeconds = System.currentTimeMillis();
                                startListenerUDP();
                                sortReadings();
                            }
                        } else if ("Stop".equalsIgnoreCase(command)) {
                            System.out.println("Stop received, exiting...");
                            udpRunning = false;
                            udpSocket.close();
                            return;
                        }
                    }

                    if (record.topic().equals("Register")) {
                        JSONObject json = new JSONObject(record.value());
                        String id = json.getString("id");
                        if (!id.equals(sensorId)) {
                            otherSensors.put(id, json);
                            System.out.println("Received registration from: " + json);
                        }
                    }
                }

                if (running) {
                    Thread.sleep(2500);
                    String currentNo2 = getCurrentNO2();
                    sendReading(currentNo2);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendRegistration() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            JSONObject json = new JSONObject();
            json.put("id", sensorId);
            json.put("address", "localhost");
            json.put("port", sensorPort);

            producer.send(new ProducerRecord<>("Register", json.toString()));
            System.out.println("Sent registration: " + json);
            producer.flush();
        }
    }

    private void startListenerUDP() {
        new Thread(() -> {
            byte[] rcvBuf = new byte[1024];

            while (udpRunning) {
                try {
                    DatagramPacket packet = new DatagramPacket(rcvBuf, rcvBuf.length);
                    udpSocket.receive(packet);

                    String rcvStr = new String(packet.getData(), packet.getOffset(), packet.getLength());
                    System.out.println("Server received: " + rcvStr);
                    JSONObject json = new JSONObject(rcvStr);
                    if (json.getString("type").equals("data")) {
                        parseData(json, packet.getPort());
                    } else if (json.getString("type").equals("ack")) {
                        parseAck(json);
                    }

                } catch (Exception e) {
                    if (!udpRunning) break;
                }
            }
        }).start();
    }

    private void sendReading(String no2Value) {
        vectorClock.put(sensorId, vectorClock.getOrDefault(sensorId, 0) + 1);
        long scalar = scalarClock.currentTimeMillis();

        JSONObject json = new JSONObject();
        json.put("type", "data");
        json.put("from", sensorId);
        json.put("no2", no2Value);
        json.put("scalarClock", scalar);
        json.put("vectorClock", new JSONObject(vectorClock));
        allReadings.add(new Reading(scalar, vectorClock, no2Value));
        byte[] data = json.toString().getBytes();

        for (JSONObject s : otherSensors.values()) {
            try {
                String receiverId = s.getString("id");
                String key = scalar + "_" + receiverId;
                receivedAcks.put(key, false);
                InetAddress addr = InetAddress.getByName("localhost");
                int port = s.getInt("port");
                DatagramPacket packet = new DatagramPacket(data, data.length, addr, port);
                udpSocket.send(packet);
                new Thread(() -> {
                    try {
                        Thread.sleep(3000);
                        if (udpRunning && !receivedAcks.get(key)) {
                            try {
                                System.out.println("Retransmitting packet " + key);
                                udpSocket.send(packet);
                            } catch (SocketException e) {
                                if (!udpRunning) return;
                                e.printStackTrace();
                            }
                        }
                    } catch (Exception ignored) {
                    }
                }).start();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private void parseData(JSONObject json, int port) {
        long receivedScalar = json.getLong("scalarClock");
        scalarClock.update(receivedScalar);
        if (receivedPackets.contains(receivedScalar)) {
            sendAck(port, receivedScalar);
            return;
        }
        receivedPackets.add(receivedScalar);
        JSONObject v = json.getJSONObject("vectorClock");
        String no2Value = json.getString("no2");
        Map<String, Integer> receivedVectorClock = new HashMap<>();
        for (String k : v.keySet()) {
            receivedVectorClock.put(k, v.getInt(k));
        }
        allReadings.add(new Reading(receivedScalar, receivedVectorClock, no2Value));
        for (String k : v.keySet()) {
            vectorClock.put(k, Math.max(vectorClock.getOrDefault(k, 0), v.getInt(k)));
        }
        vectorClock.put(sensorId, vectorClock.getOrDefault(sensorId, 0) + 1);
        sendAck(port, receivedScalar);
    }


    private void parseAck(JSONObject json) {
        long scalar = json.getLong("scalarClock");
        String fromId = json.getString("from");
        String key = scalar + "_" + fromId;
        receivedAcks.put(key, true);
        System.out.println("received ack:" + key);
    }

    private void sendAck(int port, long scalarClock) {
        try {
            JSONObject json = new JSONObject();
            json.put("type", "ack");
            json.put("from", sensorId);
            json.put("scalarClock", scalarClock);
            byte[] data = json.toString().getBytes();
            DatagramPacket p = new DatagramPacket(data, data.length, InetAddress.getByName("localhost"), port);
            udpSocket.send(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sortReadings() {
        new Thread(() -> {
            while (udpRunning) {
                try {
                    Thread.sleep(5000);
                    long now = scalarClock.currentTimeMillis();
                    long windowStart = now - 5000;
                    List<Reading> windowReadings;
                    List<Reading> snapshot;
                    synchronized (allReadings) {
                        snapshot = new ArrayList<>(allReadings);
                    }
                    windowReadings = new ArrayList<>();
                    for (Reading r : snapshot) {
                        if (r.getScalarClock() >= windowStart) {
                            windowReadings.add(r);
                        }
                    }

                    if (windowReadings.isEmpty()) {
                        System.out.println("------------------Empty!------------------");
                        continue;
                    }

                    windowReadings.sort(Comparator.comparingLong(Reading::getScalarClock));
                    System.out.println("------------------------------------------------------------");
                    System.out.println("Sorted by scalar clock:");
                    windowReadings.forEach(System.out::println);

                    windowReadings.sort((r1, r2) -> {
                        Set<String> allKeys = new HashSet<>();
                        allKeys.addAll(r1.getVectorClock().keySet());
                        allKeys.addAll(r2.getVectorClock().keySet());

                        List<String> sortedKeys = new ArrayList<>(allKeys);
                        Collections.sort(sortedKeys);

                        for (String k : sortedKeys) {
                            int diff = r1.getVectorClock().getOrDefault(k, 0) - r2.getVectorClock().getOrDefault(k, 0);
                            if (diff != 0) {
                                return diff;
                            }
                        }
                        return 0;
                    });
                    System.out.println("Sorted by vector clock:");
                    windowReadings.forEach(System.out::println);

                    double avg = windowReadings.stream().mapToDouble(r -> Double.parseDouble(r.getNo2())).average().orElse(0.0);
                    System.out.println("Average NO2 in last 5s: " + avg);
                    System.out.println("------------------------------------------------------------");

                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter sensor ID: ");
        String sensorId = scanner.nextLine();
        System.out.print("Enter sensor port: ");
        Integer port = Integer.valueOf(scanner.nextLine());
        Sensor sensor = new Sensor(sensorId, port);
        try {
            sensor.start();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }
}
