package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerStringSASL {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=admin-secret;");
        props = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new StringSerializer());

        String topic = "my-topic";

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "myKey", "myValue2");
        try {
            RecordMetadata ack = producer.send(record).get();
            System.out.printf(String.format("Send message with offset %d to topic %s - partition %d", ack.offset(), ack.topic(), ack.partition()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

}
