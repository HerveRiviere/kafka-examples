package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class ConsumerString {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-topic:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props = ConsumerConfig.addDeserializerToConfig(props,new StringDeserializer(),new StringDeserializer());



    List<String> topics_to_consume = Arrays.asList("my-topic");


    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(topics_to_consume);
      consumer.poll(0);
      HashMap<TopicPartition, Long> timePartition = new HashMap<TopicPartition, Long>();

      System.out.println(consumer.assignment());
      timePartition.put(new TopicPartition("my-topic",1),1471680079302L);
      Map<TopicPartition, OffsetAndTimestamp> offset = consumer.offsetsForTimes(timePartition);
      System.out.println(offset);

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records)
          System.out.printf("offset = %d, key = %s, partition = %s, value = %s timestamp = %s \n", record.offset(), record.key(), record.partition(), record.value(), new Date(record.timestamp()).toString());
      }
    }
  }

}
