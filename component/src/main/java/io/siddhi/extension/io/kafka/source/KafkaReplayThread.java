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
import io.siddhi.extension.io.kafka.util.KafkaReplayResponseSourceRegistry;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.kafka.source.KafkaSource.ADAPTOR_ENABLE_AUTO_COMMIT;

/**
 * This runnable processes each Kafka message and sends it to siddhi.
 */
public class KafkaReplayThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(KafkaReplayThread.class);
    private final KafkaConsumer<byte[], byte[]> consumer;
    // KafkaConsumer is not thread safe, hence we need a lock
    private final Lock consumerLock = new ReentrantLock();
    private final String partitions[];
    private SourceEventListener sourceEventListener;
    private String topics[];
    private volatile boolean paused;
    private volatile boolean inactive;
    private List<TopicPartition> partitionsList = new ArrayList<>();
    private String consumerThreadId;
    private boolean isPartitionWiseThreading = false;
    private boolean isBinaryMessage = false;
    private boolean enableOffsetCommit = false;
    private boolean enableAutoCommit = false;
    private boolean enableAsyncCommit;
    private boolean consumerClosed;
    private ReentrantLock lock;
    private Condition condition;
    private String[] requiredProperties;
    private int trpLength;
    private int startOffset;
    private int endOffset;
    private int threadId;
    private String sinkId;

    KafkaReplayThread(SourceEventListener sourceEventListener, String[] topics, String[] partitions,
                        Properties props, boolean isPartitionWiseThreading, boolean isBinaryMessage,
                        boolean enableOffsetCommit, boolean enableAsyncCommit, String[] requiredProperties,
                      int startOffset, int endOffset, int threadId, String sinkId) {
        this.threadId = threadId;
        this.sinkId = sinkId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.consumer = new KafkaConsumer<>(props);
        this.sourceEventListener = sourceEventListener;
        this.topics = topics;
        this.partitions = partitions;
        this.isPartitionWiseThreading = isPartitionWiseThreading;
        this.isBinaryMessage = isBinaryMessage;
        this.enableOffsetCommit = false;
        this.enableAutoCommit = Boolean.parseBoolean(props.getProperty(ADAPTOR_ENABLE_AUTO_COMMIT, "true"));
        this.consumerThreadId = buildId();
        this.enableAsyncCommit = enableAsyncCommit;
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
                    LOG.info("Adding partition " + partition1 + " for topic: " + topic);
                    partitionsList.add(partition);
                }
                LOG.info("Adding partitions " + Arrays.toString(partitions) + " for topic: " + topic);
                consumer.assign(partitionsList);
            }
        } else {
            consumer.subscribe(Arrays.asList(topics));
        }
        consumerClosed = false;
        LOG.info("Subscribed for topics: " + Arrays.toString(topics));
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
                consumer.seekToBeginning(partitionsList);
                records = consumer.poll(100);
            } catch (CommitFailedException ex) {
                LOG.warn("Consumer poll() failed." + ex.getMessage(), ex);
            } finally {
                consumerLock.unlock();
            }
            if (null != records) {
                for (ConsumerRecord record : records) {
                    String[] trpProperties = new String[trpLength];
                    if (!consumerClosed) {
                        if (record.offset() >= startOffset) {
                            if (record.offset() > endOffset) {
                                inactive = true;
                                KafkaReplayResponseSourceRegistry.getInstance().getKafkaReplayResponseSource(sinkId)
                                        .onReplayFinish(threadId);
                                break;
                            }
                            int partition = record.partition();
                            Object event = record.value();
                            Object eventBody = null;
                            String header = null;
                            long eventTimestamp = System.currentTimeMillis();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Event received in Kafka Event Adaptor with offSet: " + record.offset() +
                                        ", key: " + record.key() + ", topic: " + record.topic() +
                                        ", partition: " + partition + ", recordTimestamp: " + record.timestamp() +
                                        ", eventTimestamp: " + eventTimestamp + ", checksum: " + record.checksum());
                            }
                            for (int i = 0; i < requiredProperties.length; i++) {
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_PARTITION)) {
                                    trpProperties[i] = String.valueOf(record.partition());
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_TOPIC)) {
                                    trpProperties[i] = record.topic();
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_KEY)) {
                                    trpProperties[i] = String.valueOf(record.key());
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_RECORD_TIMESTAMP)) {
                                    trpProperties[i] = String.valueOf(record.timestamp());
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_EVENT_TIMESTAMP)) {
                                    trpProperties[i] = String.valueOf(eventTimestamp);
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_CHECK_SUM)) {
                                    trpProperties[i] = String.valueOf(record.checksum());
                                }
                                if (requiredProperties[i].equalsIgnoreCase(Constants.TRP_OFFSET)) {
                                    trpProperties[i] = String.valueOf(record.offset());
                                }
                            }
                            String transportSyncProperties = "topic:" + record.topic() + ",partition:"
                                    + record.partition() + ",offSet:" + record.offset();
                            String[] transportSyncPropertiesArr = new String[]{transportSyncProperties};
                            sourceEventListener.onEvent(event, trpProperties, transportSyncPropertiesArr);
                            if (record.offset() == endOffset) {
                                inactive = true;
                                KafkaReplayResponseSourceRegistry.getInstance().getKafkaReplayResponseSource(sinkId)
                                        .onReplayFinish(threadId);
                            }
                        }
                    } else {
                        break;
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

    private String buildId() {
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
}
