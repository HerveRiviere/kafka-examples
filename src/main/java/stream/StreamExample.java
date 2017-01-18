package stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;

/**
 * Created by adminuser on 15/01/17.
 */
public class StreamExample {

  public static void main(String[] args) throws Exception {

    Properties streamsConfiguration = new Properties();

    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app");

    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    //streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.J);
    //streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");


    KStreamBuilder builder = new KStreamBuilder();

    // Read the input Kafka topic into a KStream instance.
    KStream<String, String> hello_world_topic = builder.stream("my-topic");

    // Convert the values to upper case
    KStream<String, String> uppercased = hello_world_topic.mapValues(String::toUpperCase);

    KTable<String, Long> count_by_key = hello_world_topic.groupByKey().count("my-store");


    // Write the uppercased results to a new Kafka topic
    uppercased.to("UppercasedTopic");


    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}
