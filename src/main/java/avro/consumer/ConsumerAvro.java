package avro.consumer;

import avro.consumer.model.Events;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class ConsumerAvro {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("schema.registry.url", "http://localhost:8081");
    props.put("auto.offset.reset", "earliest");



    List<String> topics_to_consume = Arrays.asList("my_avro_topic");


    try (KafkaConsumer<String, Events> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(topics_to_consume);
      consumer.poll(0);
      consumer.seekToBeginning(consumer.assignment());

      while (true) {
        ConsumerRecords<String, Events> records = consumer.poll(100);
        for (ConsumerRecord<String, Events> record : records)
          System.out.printf("offset = %d, key = %s, value = %s timestamp = %s \n", record.offset(), record.key(), record.value(), new Date(record.timestamp()).toString());
      }
    }
  }

}
