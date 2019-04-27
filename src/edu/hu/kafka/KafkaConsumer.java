package edu.hu.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by bhavanishekhawat
 */
public class KafkaConsumer {

    private ConsumerConnector consumerConnector;
    private final static String TOPIC = "KAFKA_RANDOM_NUM_TOPIC";

    public void initialize() {

        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:21811");
        props.put("group.id", "testgroup");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "300");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig conConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
    }

    public void consume() {

        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(TOPIC, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                        consumerConnector.createMessageStreams(topicCount);

        List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams.get(TOPIC);

        // Keep iterating over the messages
        for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
            ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();

            while (consumerIte.hasNext())
                System.out.println("Message consumed from topic [" + TOPIC + "]:" +
                                new String(consumerIte.next().message()));
        }
        if (consumerConnector != null)
            consumerConnector.shutdown();
    }

    public static void main(String[] args) throws InterruptedException {

        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        kafkaConsumer.initialize();
        kafkaConsumer.consume();
    }

}
