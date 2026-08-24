package com.GridWeaver.config;

import com.GridWeaver.model.Zone;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Maps zone name directly to partition index via the enum ordinal.
 *
 * The default murmur2 partitioner collides badly with only five distinct keys:
 * observed A+C on one partition, B+D on another, and two partitions empty.
 * With zones fixed and partition count equal to zone count, the ordinal IS the
 * correct partition -- no hashing needed.
 */
public class ZonePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int partitions = cluster.partitionCountForTopic(topic);
        try {
            return Zone.valueOf((String) key).ordinal() % partitions;
        } catch (Exception e) {
            return 0;
        }
    }

    @Override public void close() { }

    @Override public void configure(Map<String, ?> configs) { }
}