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
import io.siddhi.extension.io.kafka.metrics.SourceMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * This processes the Kafka messages using a thread pool.
 */
public class ConsumerKafkaGroup {
    private static final Logger LOG = LogManager.getLogger(ConsumerKafkaGroup.class);
    private final String topics[];
    private final String partitions[];
    private final Properties props;
    private List<KafkaConsumerThread> kafkaConsumerThreadList = new ArrayList<>();
    private ExecutorService executorService;
    private String threadingOption;
    private boolean isBinaryMessage;
    private KafkaSource.KafkaSourceState kafkaSourceState;
    private List<Future<?>> futureList = new ArrayList<>();

    ConsumerKafkaGroup(String[] topics, String[] partitions, Properties props, String threadingOption,
                       ExecutorService executorService, boolean isBinaryMessage, boolean enableOffsetCommit,
                       boolean enableAsyncCommit, SourceEventListener sourceEventListener,
                       String[] requiredProperties, SourceMetrics metrics) {
        this.threadingOption = threadingOption;
        this.topics = topics;
        this.partitions = partitions;
        this.props = props;
        this.executorService = executorService;
        this.isBinaryMessage = isBinaryMessage;

        if (KafkaSource.SINGLE_THREADED.equals(threadingOption)) {
            KafkaConsumerThread kafkaConsumerThread =
                    new KafkaConsumerThread(sourceEventListener, topics, partitions, props,
                            false, isBinaryMessage, enableOffsetCommit, enableAsyncCommit,
                            requiredProperties, metrics);
            kafkaConsumerThreadList.add(kafkaConsumerThread);
            LOG.info("Kafka Consumer thread starting to listen on topic(s): " + Arrays.toString(topics) +
                    " with partition/s: " + Arrays.toString(partitions));
        } else if (KafkaSource.TOPIC_WISE.equals(threadingOption)) {
            for (String topic : topics) {
                KafkaConsumerThread kafkaConsumerThread =
                        new KafkaConsumerThread(sourceEventListener, new String[]{topic}, partitions, props,
                                false, isBinaryMessage, enableOffsetCommit, enableAsyncCommit,
                                requiredProperties, metrics);
                kafkaConsumerThreadList.add(kafkaConsumerThread);
                LOG.info("Kafka Consumer thread starting to listen on topic: " + topic +
                        " with partition/s: " + Arrays.toString(partitions));
            }
        } else if (KafkaSource.PARTITION_WISE.equals(threadingOption)) {
            for (String topic : topics) {
                for (String partition : partitions) {
                    KafkaConsumerThread kafkaConsumerThread =
                            new KafkaConsumerThread(sourceEventListener, new String[]{topic},
                                    new String[]{partition}, props, true,
                                    isBinaryMessage, enableOffsetCommit, enableAsyncCommit, requiredProperties,
                                    metrics);
                    kafkaConsumerThreadList.add(kafkaConsumerThread);
                    LOG.info("Kafka Consumer thread starting to listen on topic: " + topic +
                            " with partition: " + partition);
                }
            }
        }
    }

    void pause() {
        kafkaConsumerThreadList.forEach(KafkaConsumerThread::pause);
    }

    void resume() {
        kafkaConsumerThreadList.forEach(KafkaConsumerThread::resume);
    }

    void restoreState() {
        kafkaConsumerThreadList.forEach(kafkaConsumerThread -> kafkaConsumerThread.restore());
    }

    void shutdown() {
        kafkaConsumerThreadList.forEach(KafkaConsumerThread::shutdownConsumer);
        futureList.forEach(future -> {
            if (!future.isCancelled()) {
                future.cancel(true);
            }
        });
    }

    void run() {
        try {
            for (KafkaConsumerThread consumerThread : kafkaConsumerThreadList) {
                futureList.add(CompletableFuture.runAsync(consumerThread, executorService).whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                LOG.error("KafkaConsumerThread for topic(s):{} terminated unexpectedly!",
                                        Arrays.toString(topics), throwable);
                            }
                        }
                ));
            }
        } catch (Throwable t) {
            LOG.error("Error while creating KafkaConsumerThread for topic(s): " + Arrays.toString(topics), t);
        }
    }

    public void setKafkaSourceState(KafkaSource.KafkaSourceState kafkaSourceState) {
        this.kafkaSourceState = kafkaSourceState;
        for (KafkaConsumerThread consumer : kafkaConsumerThreadList) {
            consumer.setKafkaSourceState(kafkaSourceState);
        }
    }
}
