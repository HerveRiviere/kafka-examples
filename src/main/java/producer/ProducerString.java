package producer;

import  org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerString {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    props = ProducerConfig.addSerializerToConfig(props, new StringSerializer(),new StringSerializer());

    String topic = "my-topic";

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "myKey","myValue");
    producer.send(record, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception!= null)
          exception.printStackTrace();
        else
          System.out.println(String.format("offset %s",metadata.offset()));

      }
    });





  }

}
