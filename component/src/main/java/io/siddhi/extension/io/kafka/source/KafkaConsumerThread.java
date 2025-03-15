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

package io.siddhi.extension.io.kafka.source;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.kafka.Constants;
import io.siddhi.extension.io.kafka.metrics.SourceMetrics;
import io.siddhi.extension.io.kafka.sink.KafkaSink;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.kafka.source.KafkaSource.ADAPTOR_ENABLE_AUTO_COMMIT;
import static io.siddhi.extension.io.kafka.source.KafkaSource.ADAPTOR_SUBSCRIBER_GROUP_ID;

/**
 * This runnable processes each Kafka message and sends it to siddhi.
 */
public class KafkaConsumerThread implements Runnable {

    private static final Logger LOG = LogManager.getLogger(KafkaConsumerThread.class);
    final KafkaConsumer<byte[], byte[]> consumer;
    // KafkaConsumer is not thread safe, hence we need a lock
    private final Lock consumerLock = new ReentrantLock();
    private final String partitions[];
    private SourceEventListener sourceEventListener;
    private String topics[];
    private volatile boolean paused;
    private volatile boolean inactive;
    List<TopicPartition> partitionsList = new ArrayList<>();
    private String consumerThreadId;
    private boolean isPartitionWiseThreading = false;
    private boolean isBinaryMessage = false;
    private boolean enableOffsetCommit = false;
    private boolean enableAutoCommit = false;
    private boolean enableAsyncCommit;
    private boolean consumerClosed;
    private ReentrantLock lock;
    private Condition condition;
    private KafkaSource.KafkaSourceState kafkaSourceState;
    private String[] requiredProperties;
    private int trpLength;
    boolean isReplayThread = false;
    private SourceMetrics metrics;
    private String groupId; //Needed for metrics

    KafkaConsumerThread(SourceEventListener sourceEventListener, String[] topics, String[] partitions,
                        Properties props, boolean isPartitionWiseThreading, boolean isBinaryMessage,
                        boolean enableOffsetCommit, boolean enableAsyncCommit, String[] requiredProperties,
                        SourceMetrics metrics) {
        this.consumer = new KafkaConsumer<>(props);
        this.sourceEventListener = sourceEventListener;
        this.topics = topics;
        this.partitions = partitions;
        this.isPartitionWiseThreading = isPartitionWiseThreading;
        this.isBinaryMessage = isBinaryMessage;
        this.enableOffsetCommit = enableOffsetCommit;
        this.enableAutoCommit = Boolean.parseBoolean(props.getProperty(ADAPTOR_ENABLE_AUTO_COMMIT, "true"));
        this.groupId = props.getProperty(ADAPTOR_SUBSCRIBER_GROUP_ID);
        this.consumerThreadId = buildId();
        this.enableAsyncCommit = enableAsyncCommit;
        this.metrics = metrics;
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.requiredProperties = requiredProperties;
        if (requiredProperties != null && requiredProperties.length > 0) {
            trpLength = requiredProperties.length;
        } else {
            trpLength = 0;
        }
        if (null != partitions) {
            for (String topic : topics) {
                for (String partition1 : partitions) {
                    TopicPartition partition = new TopicPartition(topic, Integer.parseInt(partition1));
                    LOG.info("Adding partition {} for topic: {}", partition1, topic);
                    partitionsList.add(partition);
                }
                LOG.info("Adding partitions {} for topic: {}", Arrays.toString(partitions), topic);
                consumer.assign(partitionsList);
            }
        } else {
            consumer.subscribe(Arrays.asList(topics));
        }
        consumerClosed = false;
        LOG.info("Subscribed for topics: {}", Arrays.toString(topics));
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void restore() {
        final Lock consumerLock = this.consumerLock;
        if (kafkaSourceState != null && kafkaSourceState.getTopicOffsetMap() != null) {
            for (String topic : topics) {
                Map<Integer, Long> offsetMap = kafkaSourceState.getTopicOffsetMap().get(topic);
                if (null != offsetMap) {
                    for (Map.Entry<Integer, Long> entry : offsetMap.entrySet()) {
                        TopicPartition partition = new TopicPartition(topic, entry.getKey());
                        if (partitionsList.contains(partition)) {
                            LOG.info("Seeking partition: {} for topic: {} offset: {}", partition, topic, entry
                                    .getValue() + 1);
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
            if (paused) {
                lock.lock();
                try {
                    condition.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                }
            }
            // The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
            // immediately with any records that are available now. Must not be negative
            ConsumerRecords<byte[], byte[]> records = null;
            try {
                consumerLock.lock();
                // TODO add a huge value because, when there are so many equal group ids, the group balancing
                // takes time and if this value is small, there will be an CommitFailedException while
                // trying to retrieve data
                seekToRequiredOffset();
                records = consumer.poll(100);
            } catch (CommitFailedException ex) {
                LOG.warn("Consumer poll() failed.{}", ex.getMessage(), ex);
            } catch (Throwable t) {
                LOG.error("Consumer poll() failed.", t);
            } finally {
                consumerLock.unlock();
            }
            if (null != records) {
                Map<SequenceKey, Integer> lastReceivedSeqNoMap = null;
                if (!isReplayThread && kafkaSourceState.getConsumerLastReceivedSeqNoMap() != null) {
                    lastReceivedSeqNoMap = kafkaSourceState.getConsumerLastReceivedSeqNoMap().get(consumerThreadId);
                }
                for (ConsumerRecord record : records) {
                    String[] trpProperties = new String[trpLength];
                    int partition = record.partition();
                    long offset = record.offset();
                    String topic = record.topic();
                    long recordTimestamp = record.timestamp();
                    if (!consumerClosed) {
                        if (isRecordAfterStartOffset(record)) {
                            Object event = isBinaryMessage ? ((Bytes) record.value()).get() : record.value();
                            Object eventBody = null;
                            String header = null;
                            long eventTimestamp = System.currentTimeMillis();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Event received in Kafka Event Adaptor with offSet: {}, key: {}," +
                                          " topic: {}, partition: {}, recordTimestamp: {}, eventTimestamp: {}," +
                                          " checksum: {}", offset, record.key(), topic, partition,
                                        recordTimestamp, record.checksum());
                            }
                            for (int i = 0; i < requiredProperties.length; i++) {
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_PARTITION)) {
                                    trpProperties[i] = String.valueOf(partition);
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_TOPIC)) {
                                    trpProperties[i] = topic;
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_KEY)) {
                                    trpProperties[i] = String.valueOf(record.key());
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_RECORD_TIMESTAMP)) {
                                    trpProperties[i] = String.valueOf(recordTimestamp);
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_EVENT_TIMESTAMP)) {
                                    trpProperties[i] = String.valueOf(eventTimestamp);
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_CHECK_SUM)) {
                                    trpProperties[i] = String.valueOf(record.checksum());
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_OFFSET)) {
                                    trpProperties[i] = String.valueOf(offset);
                                }
                            }
                            String transportSyncProperties = "topic:" + topic + ",partition:" + partition
                                    + ",offSet:" + offset;
                            String[] transportSyncPropertiesArr = new String[]{transportSyncProperties};
                            if (lastReceivedSeqNoMap == null) {
                                sourceEventListener.onEvent(event, trpProperties, transportSyncPropertiesArr);
                            } else {
                                if (isBinaryMessage) {
                                    byte[] byteEvents = (byte[]) event;
                                    int stringSize = ByteBuffer.wrap(byteEvents).getInt();
                                    header = new String(byteEvents, 4, stringSize - 1, Charset.defaultCharset());
                                    eventBody = Arrays.copyOfRange(byteEvents, stringSize + 4,
                                            byteEvents.length);
                                } else {
                                    String stringEvent = event.toString();
                                    int headerStartingIndex = stringEvent.indexOf(KafkaSink.SEQ_NO_HEADER_DELIMITER);
                                    eventBody = stringEvent.substring(headerStartingIndex + 1);
                                    if (headerStartingIndex > 0) {
                                        header = stringEvent.substring(0, headerStartingIndex);
                                        }
                                    }
                                    if (null != header && !header.isEmpty()) {
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
                                            sourceEventListener.onEvent(eventBody, trpProperties,
                                                    transportSyncPropertiesArr);
                                            if (LOG.isDebugEnabled()) {
                                                LOG.debug("Last Received SeqNo Updated to:{} for SeqKey:[{}] " +
                                                          "in Kafka consumer thread:{}", seqNo,
                                                          sequenceKey.toString(), consumerThreadId);
                                            }
                                        } else {
                                            if (LOG.isDebugEnabled()) {
                                                LOG.debug("Duplicate Message arrived at Kafka Consumer Thread:{}. " +
                                                          "SeqKey:[{}], Latest SeqNo:{}, this message SeqNo:{}." +
                                                          " Ignoring the message.", consumerThreadId,
                                                          sequenceKey.toString(), lastReceivedSeqNo, seqNo);
                                            }
                                        }

                                    } else {
                                        LOG.warn("'Sequenced' option is set to true in Kafka source configuration." +
                                                 " But this message does not contain the sequence number in " +
                                                 "consumer thread :{}. Dropping the message", consumerThreadId);
                                    }
                                }
                                if (!isReplayThread) {
                                    kafkaSourceState.getTopicOffsetMap().get(record.topic())
                                            .putIfAbsent(record.partition(), record.offset());
                                }
                                if (endReplay(record)) {
                                    inactive = true;
                                    break;
                                }
                            }
                    } else {
                        if (!isReplayThread) {
                            kafkaSourceState.getTopicOffsetMap().get(record.topic()).putIfAbsent(record.partition(),
                                  record.offset());
                        }
                        if (metrics != null) {
                            updateMetrics(topic, partition, recordTimestamp);
                        }
                        break;
                    }
                }
                if (!isReplayThread && enableOffsetCommit && !enableAutoCommit) {
                    try {
                        consumerLock.lock();
                        if (!records.isEmpty()) {
                            if (enableAsyncCommit) {
                                consumer.commitAsync(new KafkaOffsetCommitCallback());
                            } else {
                                try {
                                    consumer.commitSync();
                                } catch (KafkaException e) {
                                    if (metrics != null) {
                                        for (String topic: topics) {
                                            metrics.getErrorCountPerStream(topic, groupId, "KafkaException").inc();
                                        }
                                    }
                                    LOG.error("Exception occurred when committing offsets Synchronously", e);
                                }
                            }
                        }
                    } catch (CommitFailedException e) {
                        if (metrics != null) {
                            for (String topic: topics) {
                                metrics.getErrorCountPerStream(topic, groupId, "CommitFailedException").inc();
                            }
                        }
                        LOG.error("Kafka commit failed for topic kafka_result_topic", e);
                    } finally {
                        consumerLock.unlock();
                    }
                }
            }
            try { //To avoid thread spin
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void seekToRequiredOffset() {}

    boolean isRecordAfterStartOffset(ConsumerRecord record) {
        Map<Integer, Long> partitionMap = kafkaSourceState.getTopicOffsetMap().get(record.topic());
        if (partitionMap == null) {
            return true;
        }

        Long offsetThreshold = partitionMap.get(record.partition());
        return offsetThreshold == null || record.offset() > offsetThreshold;
    }

    boolean endReplay(ConsumerRecord record) {
        return false;
    }

    void shutdownConsumer() {
        try {
            consumerLock.lock();
            consumer.close();
            consumerClosed = true;
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

    public void setKafkaSourceState(KafkaSource.KafkaSourceState kafkaSourceState) {
        this.kafkaSourceState = kafkaSourceState;
        if (kafkaSourceState != null) {
            if (kafkaSourceState.getConsumerLastReceivedSeqNoMap() != null) {
                kafkaSourceState.getConsumerLastReceivedSeqNoMap().putIfAbsent(consumerThreadId, new HashMap<>());
            }
            for (String topic : topics) {
                kafkaSourceState.getTopicOffsetMap().putIfAbsent(topic, new HashMap<>());
            }
        }
    }

    private static class KafkaOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception == null) {
                if (LOG.isDebugEnabled()) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        LOG.debug("Asynchronously commit offset done for {} with offset of: {}", entry.getKey().topic(),
                                entry.getValue().offset());
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        LOG.debug("Commit offset exception for {} with offset of: {}", entry.getKey().topic(),
                                entry.getValue().offset());
                    }
                }
                LOG.error("Exception occurred when committing offsets asynchronously.", exception);
            }
        }
    }

    private void updateMetrics(String topic, int partition, long recordTimestamp) {
        metrics.getTotalReads().inc();
        metrics.getCurrentOffset(topic, partition, groupId);
        metrics.getReadCountPerStream(topic, partition, groupId).inc();
        metrics.getLastMessageConsumedTime(topic, groupId);
        metrics.getConsumerLag(topic, groupId, partition, recordTimestamp);
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
