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

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This processes the Kafka messages using a thread pool.
 */
public class ConsumerKafkaGroup {
    private static final Logger LOG = Logger.getLogger(ConsumerKafkaGroup.class);
    private final String topics[];
    private final String partitions[];
    private final Properties props;
    private List<KafkaConsumerThread> kafkaConsumerThreadList = new ArrayList<>();
    private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
    private ScheduledExecutorService executorService;
    private String threadingOption;
    private Map<String, Map<SequenceKey, Integer>> perConsumerLastReceivedSeqNo = new HashMap<>();

    ConsumerKafkaGroup(String topics[], String partitions[], Properties props, Map<String, Map<Integer, Long>>
            topicOffsetMap, Map<String, Map<SequenceKey, Integer>> perConsumerLastReceivedSeqNo, String threadingOption,
                       ScheduledExecutorService executorService) {
        this.threadingOption = threadingOption;
        this.topicOffsetMap = topicOffsetMap;
        this.perConsumerLastReceivedSeqNo = perConsumerLastReceivedSeqNo;
        this.topics = topics;
        this.partitions = partitions;
        this.props = props;
        this.executorService = executorService;
    }

    void pause() {
        kafkaConsumerThreadList.forEach(KafkaConsumerThread::pause);
    }

    void resume() {
        kafkaConsumerThreadList.forEach(KafkaConsumerThread::resume);
    }

    void restore(final Map<String, Map<Integer, Long>> topic) {
        kafkaConsumerThreadList.forEach(kafkaConsumerThread -> kafkaConsumerThread.restore(topic));
    }

    void shutdown() {
        kafkaConsumerThreadList.forEach(KafkaConsumerThread::shutdownConsumer);
    }

    void run(SourceEventListener sourceEventListener) {
        try {
            if (KafkaSource.SINGLE_THREADED.equals(threadingOption)) {
                KafkaConsumerThread kafkaConsumerThread =
                        new KafkaConsumerThread(sourceEventListener, topics, partitions, props, topicOffsetMap, false);
                kafkaConsumerThreadList.add(kafkaConsumerThread);
                LOG.info("Kafka Consumer thread starting to listen on topic/s: " + Arrays.toString(topics) +
                        " with partition/s: " + Arrays.toString(partitions));
            } else if (KafkaSource.TOPIC_WISE.equals(threadingOption)) {
                for (String topic : topics) {
                    KafkaConsumerThread kafkaConsumerThread =
                            new KafkaConsumerThread(sourceEventListener, new String[]{topic}, partitions, props,
                                    topicOffsetMap, false);
                    kafkaConsumerThreadList.add(kafkaConsumerThread);
                    LOG.info("Kafka Consumer thread starting to listen on topic: " + topic +
                            " with partition/s: " + Arrays.toString(partitions));
                }
            } else if (KafkaSource.PARTITION_WISE.equals(threadingOption)) {
                for (String topic : topics) {
                    for (String partition : partitions) {
                        KafkaConsumerThread kafkaConsumerThread =
                                new KafkaConsumerThread(sourceEventListener, new String[]{topic},
                                        new String[]{partition}, props, topicOffsetMap, true);
                        kafkaConsumerThreadList.add(kafkaConsumerThread);
                        LOG.info("Kafka Consumer thread starting to listen on topic: " + topic +
                                " with partition: " + partition);
                    }
                }
            }

            for (KafkaConsumerThread consumerThread : kafkaConsumerThreadList) {
                if (perConsumerLastReceivedSeqNo != null) {
                    Map<SequenceKey, Integer> seqNoMap = perConsumerLastReceivedSeqNo
                            .get(consumerThread.getConsumerThreadId());
                    seqNoMap = (seqNoMap != null) ? seqNoMap : new HashMap<>();
                    consumerThread.setLastReceivedSeqNoMap(seqNoMap);
                }
                executorService.submit(consumerThread);
            }
        } catch (Throwable t) {
            LOG.error("Error while creating KafkaConsumerThread for topic/s: " + Arrays.toString(topics), t);
        }
    }

    public Map<String, Map<Integer, Long>> getTopicOffsetMap() {
        Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
        for (KafkaConsumerThread kafkaConsumerThread : kafkaConsumerThreadList) {
            Map<String, Map<Integer, Long>> topicOffsetMapTemp = kafkaConsumerThread.getTopicOffsetMap();
            for (Map.Entry<String, Map<Integer, Long>> entry : topicOffsetMapTemp.entrySet()) {
                topicOffsetMap.put(entry.getKey(), entry.getValue());
            }
        }
        return topicOffsetMap;
    }

    public Map<String, Map<SequenceKey, Integer>> getPerConsumerLastReceivedSeqNo() {
        if (perConsumerLastReceivedSeqNo != null) {
            for (KafkaConsumerThread consumer : kafkaConsumerThreadList) {
                perConsumerLastReceivedSeqNo.put(consumer.getConsumerThreadId(), consumer.getLastReceivedSeqNoMap());
            }
        }
        return perConsumerLastReceivedSeqNo;
    }
}
