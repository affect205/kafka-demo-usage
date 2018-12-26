package demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static demo.App.msgCounter;
import static java.lang.Thread.currentThread;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.runAsync;

public class KConsumer {
    private final String servers;
    private final String topic;
    private final String group;
    private final String client;
    private final KafkaConsumer consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    public KConsumer(final String servers, final String topic, final String group, final String client) {
        this.servers = servers;
        this.topic = topic;
        this.group = group;
        this.client = client;
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", group);
        props.put("client.id", client);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            runAsync(() -> {
                System.out.printf("start consumer %s...\n", client);
                try {
                    consumer.subscribe(singletonList(topic));
                    System.out.println("Subscribed to topic=" + topic + ", group=" + group + ", clientId=" + client);
                    while (isRunning.get()) {
                        ConsumerRecords<String, String> records = consumer.poll(100);
                        for (ConsumerRecord<String, String> record : records) {
                            System.out.printf("[%s]: threadid=%s, partition=%s, offset=%d, key=%s, value=%s,  time=%s\n",
                                    client, currentThread().getId(), record.partition(), record.offset(), record.key(), record.value(), sdf.format(new Date()));
                            System.out.println("messages = " + msgCounter.incrementAndGet());
                        }
                    }
                } finally {
                    System.out.printf("[%s]:unsubscribe from topic %s\n", client, topic);
                    consumer.unsubscribe();
                    isRunning.compareAndSet(true, false);
                }
            });
        } else {
            System.out.printf("consumer %s already started!\n", client);
        }
    }

    public void stop() {
        System.out.printf("stop consumer %s...\n", client);
        isRunning.compareAndSet(true, false);
    }
}
