package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import static demo.HashUtils.hashSHA256;
import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.StreamsConfig.*;

public class KStreamWordCounterDuplicates {
    public static final String INPUT_TOPIC = "streams-wordcount-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-duplicates-output";

    public static void main(final String[] args) throws Exception {
        final String bootstrapServers = "kafka1:9092";
        final Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "wordcount-" + UUID.randomUUID().toString()); // consumer group indeed
        config.put(CLIENT_ID_CONFIG, "wordcount-demo-client");
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        config.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, 256*1024); // aggregate functions dont work without cache
        config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(INPUT_TOPIC);
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final KTable<String, String> uniqueWords = textLines
                .peek((k, line) -> System.out.println(line))
                .flatMapValues(value -> asList(pattern.split(value.toLowerCase())))
                .map((key, word) -> pair(hashSHA256(word), word))
                .groupBy((key, word) -> key)
                .reduce((aggVal, newVal) -> aggVal);
        uniqueWords.filter((((key, value) -> {
            System.out.printf("key=%s, value=%s\n", key, value);
            return true;
        })));

        //wordCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}