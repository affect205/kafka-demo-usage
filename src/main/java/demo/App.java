package demo;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class App {
    public static AtomicInteger msgCounter = new AtomicInteger(0);
    public static void main(String[] args) throws Exception {
        String servers = "localhost:9092";
        String topic = "test2";
        String group = "group-x";
        int poolSize = 3;
        System.out.println("start kafka consumer pool...");
        System.out.printf("servers=%s, topic=%s, group=%s, poolSize=%s\n", servers, topic, group, poolSize);
        ConsumerPool pool = ConsumerPool.create(servers, topic, group, poolSize);
        pool.start();
        Collection<String> ids = pool.getClientIds();
        System.out.printf("available clients: %s\n", Arrays.toString(ids.toArray()));
        String stopId = ids.iterator().next();
        Thread.sleep(5000);
        pool.stop(stopId);
        Thread.sleep(5000);
        pool.start(stopId);
        Thread.sleep(600000);
    }
}
