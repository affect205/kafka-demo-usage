package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.valueOf;
import static java.util.UUID.randomUUID;

public class KProducer {

    public static void main(String[] args) throws Exception {
        //Assign topicName to string variable
        String topicName = "kafka-migration-topic";
        System.out.println("Producer topic=" + topicName);

        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "kafka1:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 10);
        props.put("enable.idempotence", true);
        Producer<String, String> producer = new KafkaProducer(props);

        System.out.println("Enter key:value, q - Exit");

        for (int i=0; i < 100; ++i) {
            String key = valueOf(ThreadLocalRandom.current().nextInt(16));
            String val = "{\n" +
                    "  \"msisdn\": \"78005551534\",\n" +
                    "  \"billingServiceId\": 125,\n" +
                    "  \"isCheated\": false,\n" +
                    "  \"dateRegistration\": null,\n" +
                    "  \"maxContactCount\": 1,\n" +
                    "  \"friends\": [{\n" +
                    "    \"name\": \"bratyunya\",\n" +
                    "    \"msisdn\": \"79854404311\"\n" +
                    "  }],\n" +
                    "  \"childs\": [],\n" +
                    "  \"zone\": [{\n" +
                    "    \"name\": \"TEKHNOPARK\",\n" +
                    "    \"latitude\": 37.66,\n" +
                    "    \"longitude\": 55.69,\n" +
                    "    \"radius\": 25\n" +
                    "  }]\n" +
                    "}";
            System.out.printf("Send message: %s:%s:%s\n", i, key, val);
            // strategy by round
            //producer.send(new ProducerRecord(topicName, split[0]));
            // strategy by hash
            producer.send(new ProducerRecord(topicName, key, val));
            // strategy by partition
            //producer.send(new ProducerRecord(topicName, Integer.valueOf(split[2]), split[0], split[1]));
            Thread.sleep(5);
        }
    }
}