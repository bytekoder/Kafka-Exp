package edu.hu.kafka;

import kafka.producer.Partitioner;

/**
 * Created by bhavanishekhawat
 */
public class KafkaPartitioner implements Partitioner {

    @Override
    public int partition(Object key, int numPartitions) {

        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt(stringKey.substring(offset + 1)) % numPartitions;
        }
        return partition;
    }
}
