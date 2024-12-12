import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.gson.Gson;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TwitchNetworkProducer {
    private static final String TOPIC_NAME = "twitch-network-rishireddy";
    private static KafkaProducer<String, String> producer;
    private static final Random random = new Random();
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: TwitchNetworkProducer <kafka-brokers>");
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        producer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new GenerateAndSendDataTask(), 0, 1, TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producer...");
            producer.close();
            scheduler.shutdown();
        }));
    }

    static class NetworkEdge {
        long user_id_1;
        long user_id_2;
        long timestamp;

        NetworkEdge(long user1, long user2) {
            this.user_id_1 = user1;
            this.user_id_2 = user2;
            this.timestamp = System.currentTimeMillis();
        }
    }

    static class GenerateAndSendDataTask implements Runnable {
        @Override
        public void run() {
            try {
                // Generate 10 edges per batch
                for (int i = 0; i < 10; i++) {
                    long user1 = random.nextInt(1000);
                    long user2 = random.nextInt(1000);

                    NetworkEdge edge = new NetworkEdge(user1, user2);
                    String jsonData = gson.toJson(edge);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC_NAME, jsonData);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Error sending message: " + exception.getMessage());
                        } else {
                            System.out.println("Sent network edge: " + jsonData);
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}