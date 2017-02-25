package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class ConsumerString {

  public static void main(String[] args) {
    Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props = ConsumerConfig.addDeserializerToConfig(props,new StringDeserializer(),new StringDeserializer());



    List<String> topics_to_consume = Arrays.asList("my-topic");

      KafkaConsumer<String, String> consumer = null;

      try {
          consumer = new KafkaConsumer<>(props);
          consumer.subscribe(topics_to_consume);
          consumer.poll(0);
          HashMap<TopicPartition, Long> timePartition = new HashMap<TopicPartition, Long>();

          //Look at all assignated partitions
          Iterator<TopicPartition> partitions = consumer.assignment().iterator();
          while (partitions.hasNext()) {
              TopicPartition partition = partitions.next();
              System.out.println(String.format("Got partition %d assignated from topic : %s", partition.partition(), partition.topic()));
              timePartition.put(partition, 0L);//seek earliest offet
          }

          //Go to earliest offset for each partition
          Iterator<Map.Entry<TopicPartition, OffsetAndTimestamp>> offsets = consumer.offsetsForTimes(timePartition).entrySet().iterator();
          while (offsets.hasNext()) {
              Map.Entry<TopicPartition, OffsetAndTimestamp> offset = offsets.next();
              System.out.println(String.format("Consumer will go to : Topic %s partition %d : offset : %d", offset.getKey().topic(), offset.getKey().partition(), offset.getValue().offset()));
              consumer.seek(offset.getKey(), offset.getValue().offset());
          }


          //Consume message
          while (true) {
              ConsumerRecords<String, String> records = consumer.poll(100);
              for (ConsumerRecord<String, String> record : records)
                  System.out.printf("offset = %d, key = %s, partition = %s, value = %s timestamp = %s \n", record.offset(), record.key(), record.partition(), record.value(), new Date(record.timestamp()).toString());
          }
      } finally {
          consumer.close();
      }
  }

}
