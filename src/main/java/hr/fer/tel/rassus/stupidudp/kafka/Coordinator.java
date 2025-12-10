package hr.fer.tel.rassus.stupidudp.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class Coordinator {

    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties)) {

            producer.send(new ProducerRecord<>("Command", "Start"));
            System.out.println("Sent Start on topic Command");
            try {
                Thread.sleep(120000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new ProducerRecord<>("Command", "Stop"));
            System.out.println("Sent Stop command");
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
