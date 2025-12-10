package hr.fer.tel.rassus.stupidudp.sensor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.time.Duration;

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
    private long startTime;
    private String sensorId;
    private String sensorPort;
    private Map<String, JSONObject> otherSensors = new HashMap<>();


    public Sensor(String sensorId, String port) {
        this.lines = new ArrayList<>();
        this.startTime = System.currentTimeMillis();
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
        long seconds = (System.currentTimeMillis() - startTime) / 1000;
        int rowIndex = (int) (seconds % 100) + 1;
        String[] row = lines.get(rowIndex);
        String value = "0";
        if (!row[4].isEmpty()) {
            value = row[4];
        }
        return value;
    }

    public void start() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Sensor " + sensorId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
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
                            }
                        } else if ("Stop".equalsIgnoreCase(command)) {
                            System.out.println("Stop received, exiting...");
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
                    String currentNo2 = getCurrentNO2();
                    System.out.println("current no2:" + currentNo2);
                }
            }
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

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter sensor ID: ");
        String sensorId = scanner.nextLine();
        //System.out.print("Enter Node address: ");
        //String address = scanner.nextLine();
        System.out.print("Enter sensor port: ");
        String port = scanner.nextLine();
        Sensor sensor = new Sensor(sensorId, port);
        //Node node = new Node(nodeId, address, port);
        sensor.start();
    }
}

