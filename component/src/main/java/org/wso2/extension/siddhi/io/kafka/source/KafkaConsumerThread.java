/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.kafka.source;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.kafka.sink.KafkaSink;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This runnable processes each Kafka message and sends it to siddhi.
 */
public class KafkaConsumerThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(KafkaConsumerThread.class);
    private final KafkaConsumer<byte[], byte[]> consumer;
    // KafkaConsumer is not thread safe, hence we need a lock
    private final Lock consumerLock = new ReentrantLock();
    private final String partitions[];
    private SourceEventListener sourceEventListener;
    private String topics[];
    private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
    private volatile boolean paused;
    private volatile boolean inactive;
    private List<TopicPartition> partitionsList = new ArrayList<>();
    private Map<SequenceKey, Integer> lastReceivedSeqNoMap = null;
    private String consumerThreadId;
    private boolean isPartitionWiseThreading = false;

    KafkaConsumerThread(SourceEventListener sourceEventListener, String topics[], String partitions[],
                        Properties props, Map<String, Map<Integer, Long>> topicOffsetMap,
                        boolean isPartitionWiseThreading) {
        this.consumer = new KafkaConsumer<>(props);
        this.sourceEventListener = sourceEventListener;
        this.topicOffsetMap = topicOffsetMap;
        this.topics = topics;
        this.partitions = partitions;
        this.isPartitionWiseThreading = isPartitionWiseThreading;
        this.consumerThreadId = buildId();
        if (null != partitions) {
            for (String topic : topics) {
                if (null == topicOffsetMap.get(topic)) {
                    this.topicOffsetMap.put(topic, new HashMap<>());
                }
                for (String partition1 : partitions) {
                    TopicPartition partition = new TopicPartition(topic, Integer.parseInt(partition1));
                    LOG.info("Adding partition " + partition1 + " for topic: " + topic);
                    partitionsList.add(partition);
                }
                LOG.info("Adding partitions " + Arrays.toString(partitions) + " for topic: " + topic);
                consumer.assign(partitionsList);
            }
            restore(topicOffsetMap);
        } else {
            for (String topic : topics) {
                if (null == topicOffsetMap.get(topic)) {
                    this.topicOffsetMap.put(topic, new HashMap<>());
                }
            }
            consumer.subscribe(Arrays.asList(topics));
        }
        LOG.info("Subscribed for topics: " + Arrays.toString(topics));
    }

    void pause() {
        paused = true;
    }

    void resume() {
        restore(topicOffsetMap);
        paused = false;
    }

    void restore(Map<String, Map<Integer, Long>> topicOffsetMap) {
        final Lock consumerLock = this.consumerLock;
        if (null != topicOffsetMap) {
            for (String topic : topics) {
                Map<Integer, Long> offsetMap = topicOffsetMap.get(topic);
                if (null != offsetMap) {
                    for (Map.Entry<Integer, Long> entry : offsetMap.entrySet()) {
                        TopicPartition partition = new TopicPartition(topic, entry.getKey());
                        if (partitionsList.contains(partition)) {
                            LOG.info("Seeking partition: " + partition + " for topic: " + topic + " offset: " + (entry
                                    .getValue() + 1));
                            try {
                                consumerLock.lock();
                                consumer.seek(partition, entry.getValue() + 1);
                            } finally {
                                consumerLock.unlock();
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        final Lock consumerLock = this.consumerLock;
        while (!inactive) {
            while (!paused) {
                // The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
                // immediately with any records that are available now. Must not be negative
                ConsumerRecords<byte[], byte[]> records = null;
                try {
                    consumerLock.lock();
                    // TODO add a huge value because, when there are so many equal group ids, the group balancing
                    // takes time and if this value is small, there will be an CommitFailedException while
                    // trying to retrieve data
                    records = consumer.poll(100);
                } catch (CommitFailedException ex) {
                    LOG.warn("Consumer poll() failed." + ex.getMessage(), ex);
                } finally {
                    consumerLock.unlock();
                }
                if (null != records) {

                    for (ConsumerRecord record : records) {
                        int partition = record.partition();
                        String event = record.value().toString();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Event received in Kafka Event Adaptor: " + event + ", offSet: " + record.offset()
                                    + ", key: " + record.key() + ", topic: " + record.topic()
                                    + ", partition: " + partition);
                        }
                        topicOffsetMap.get(record.topic()).put(record.partition(), record.offset());

                        if (lastReceivedSeqNoMap == null) {
                            sourceEventListener.onEvent(event, new String[0]);
                        } else {
                            int headerStartingIndex = event.indexOf(KafkaSink.SEQ_NO_HEADER_DELIMITER);
                            if (headerStartingIndex > 0) {
                                String header = event.substring(0, headerStartingIndex);
                                String[] headerElements = header.split(KafkaSink.SEQ_NO_HEADER_FIELD_SEPERATOR);
                                String sequenceId = headerElements[0];
                                Integer seqNo = Integer.parseInt(headerElements[1]);
                                SequenceKey sequenceKey = new SequenceKey(sequenceId, partition);
                                Integer lastReceivedSeqNo = lastReceivedSeqNoMap.get(sequenceKey);

                                if (lastReceivedSeqNo == null) {
                                    lastReceivedSeqNo = -1;
                                }

                                if (lastReceivedSeqNo < seqNo) {
                                    lastReceivedSeqNoMap.put(sequenceKey, seqNo);
                                    String eventBody = event.substring(headerStartingIndex + 1);
                                    sourceEventListener.onEvent(eventBody, new String[0]);
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Last Received SeqNo Updated to:" + seqNo + " for " +
                                            "SeqKey:[" + sequenceKey.toString() + "] in Kafka consumer thread:"
                                            + consumerThreadId);
                                    }
                                } else {
                                    if (LOG.isDebugEnabled()) {
                                    LOG.debug("Duplicate Message arrived at Kafka Consumer Thread:" +
                                            consumerThreadId + ". SeqKey:[" + sequenceKey.toString() + "]"
                                            + ", Latest SeqNo:" + lastReceivedSeqNo +
                                            ", this message SeqNo:" + seqNo + ". Ignoring the message.");
                                    }
                                }
                            } else {
                                LOG.warn("'Sequenced' option is set to true in Kafka source configuration. " +
                                        "But this message does not contain the sequence number in consumer thread :"
                                        + consumerThreadId + ". Dropping the message");
                            }
                        }
                    }
                    try {
                        consumerLock.lock();
                        if (!records.isEmpty()) {
                            consumer.commitAsync();
                        }
                    } catch (CommitFailedException e) {
                        LOG.error("Kafka commit failed for topic kafka_result_topic", e);
                    } finally {
                        consumerLock.unlock();
                    }
                }
            }
        }
    }

    void shutdownConsumer() {
        try {
            consumerLock.lock();
            consumer.close();
        } finally {
            consumerLock.unlock();
        }
        inactive = true;
    }

    public String buildId() {
        StringBuilder key = new StringBuilder();
        int count = topics.length - 1;
        for (String topic : topics) {
            key.append(topic);
            if (--count >= 0) {
                key.append(":");
            }
        }


        if (partitions != null && isPartitionWiseThreading) {
            count = partitions.length - 1;
            key.append("-");
            for (String partition : partitions) {
                key.append(partition);
                if (--count >= 0) {
                    key.append(":");
                }
            }
        }
        return key.toString();
    }

    Map<String, Map<Integer, Long>> getTopicOffsetMap() {
        return topicOffsetMap;
    }

    public String getConsumerThreadId() {
        return consumerThreadId;
    }

    public Map<SequenceKey, Integer> getLastReceivedSeqNoMap() {
        return lastReceivedSeqNoMap;
    }

    public void setLastReceivedSeqNoMap(Map<SequenceKey, Integer> seqNoMap) {
        this.lastReceivedSeqNoMap = seqNoMap;
    }
}

/**
 * This class represents the key which message sequences are tracked against. Any two consequent Messages having the
 * same sequence key must have increasing sequence numbers.
 * SequenceKey consists of the SequenceId which is a unique identifier for each kafka Sink and the kafka partition Id.
 */
class SequenceKey {
    private String sequenceId;
    private int partitionId;

    public SequenceKey(String sequenceId, int partitionId) {
        this.sequenceId = sequenceId;
        this.partitionId = partitionId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hash = 1;
        hash = prime * hash + (sequenceId == null ? 0 : sequenceId.hashCode());
        hash = prime * hash + partitionId;
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }

        if (!(object instanceof SequenceKey)) {
            return false;
        }

        SequenceKey sequenceKey = (SequenceKey) object;

        return ((sequenceKey.sequenceId.equals(this.sequenceId)) &&
                (sequenceKey.partitionId == this.partitionId));
    }

    @Override
    public String toString() {
        return "SeqId:" + sequenceId + ", Partition" + ":" + partitionId;
    }
}
