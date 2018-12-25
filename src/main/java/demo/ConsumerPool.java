package demo;

import javax.inject.Singleton;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

@Singleton
public class ConsumerPool {
    private Map<String, KConsumer> pool = new HashMap<>();
    private ConsumerPool(String servers, String topic, String group, int poolSize) {
        IntStream.range(0, poolSize).forEach(ndx -> {
            String clientId = "client-" + UUID.randomUUID().toString();
            pool.put(clientId, new KConsumer(servers, topic, group, clientId));
        });
    }
    public void start() {
        pool.values().forEach(KConsumer::start);
    }
    public void stop() {
        pool.values().forEach(KConsumer::stop);
    }
    public void start(final String clientId) {
        if (pool.containsKey(clientId)) {
            pool.get(clientId).start();
        }
    }
    public void stop(final String clientId) {
        if (pool.containsKey(clientId)) {
            pool.get(clientId).stop();
        }
    }
    public Collection<String> getClientIds() {
        return pool.keySet();
    }
    public static ConsumerPool create(String servers, String topic, String group, int poolSize) {
        return new ConsumerPool(servers, topic, group, poolSize);
    }
}
