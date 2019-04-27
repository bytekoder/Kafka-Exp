package edu.hu.kafka;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Created by bhavanishekhawat
 */
public class KafkaProducer {

    final static String TOPIC = "KAFKA_RANDOM_NUM_TOPIC";

    /**
     * Created a separate method for topic creation due to encountering race conditions.
     * If you were to run the main method twice, the functionality would end up to be the same
     * however during the first time, I was encountering errors related to TOPIC_NOT_FOUND.
     *
     * @param topic
     * @param partitions
     * @param replication
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public static void createTopic(String topic, int partitions, int replication)
                    throws TimeoutException, InterruptedException {

        String zookeeperConnect = "localhost:21811";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;

        ZkClient zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs,
                        ZKStringSerializer$.MODULE$);

        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect),isSecureKafkaCluster);

        Properties topicConfig = new Properties();
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
        zkClient.close();

    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9043 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);

        try {
            createTopic(TOPIC, 1, 1);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Producer<String, String> producer = new Producer<String, String>(config);

        TimerTask task = new TimerTask() {

            @Override
            public void run() {

                Random random = new Random();
                int answer = random.nextInt(10) + 1;
                String convertedAnswer = Integer.toString(answer);
                KeyedMessage<String, String> message =
                                new KeyedMessage<String, String>(TOPIC, convertedAnswer);
                producer.send(message);
            }
        };

        Timer timer = new Timer();
        timer.schedule(task, new Date(), 1000);
    }
}
