/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.kafka.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric class for Kafka Sink
 */
public class SinkMetrics extends Metrics {
    private Map<String, Map<Integer, Long>> offsetMap = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, Long>> latencyMap = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, Double>> messageSizeMap = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, Long>> lastMessagePublishedTimeMap = new ConcurrentHashMap<>();

    public SinkMetrics(String siddhiAppName, String streamId) {
        super(siddhiAppName, streamId);
    }

    //Overall - To count the total writes from siddhi app level.
    public Counter getTotalWrites() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Writes.%s", siddhiAppName, "kafka"),
                        Level.INFO);
    }

    //App level - To count the total reads from app level.
    public Counter getTotalSourceWrites() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Total.Writes",
                        siddhiAppName), Level.INFO);
    }

    //topic level
    public Counter getWriteCountPerTopic(String topic) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Writes.Per.Topic.%s",
                        siddhiAppName, topic), Level.INFO);
    }

    //partition level
    public Counter getWriteCountPerPartition(String topic, int partition) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Writes.Per.Partition.%s.%s",
                        siddhiAppName, topic, "partition." + partition), Level.INFO);
    }

    //stream level
    public Counter getWriteCountPerStream(String streamId, String topic, int partition) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Writes.Per.Stream.%s.%s.%s",
                        siddhiAppName, topic, "stream_id." + streamId, "partition." + partition)
                        , Level.INFO);
    }

    //app stats
    public Counter getErrorCountPerTopic(String topic, String errorString) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Errors.Per.Topic.%s.%s",
                        siddhiAppName, topic, "errorString." + errorString), Level.INFO);
    }

    //topic
    public Counter getErrorCountPerPartition(String topic, int partition, String errorString) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Errors.Per.Partition.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition, "errorString." + errorString), Level.INFO);
    }

    //partition level
    public Counter getErrorCountPerStream(String streamId, String topic, int partition, String errorString) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Errors.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "stream_id." + streamId, "partition." + partition, "errorString." +
                                errorString), Level.INFO);
    }

    //Message size in bytes
    public void getLastMessageSize(String topic, int partition, String streamId, double messageSize) {
        updateMessageSizeMap(topic, partition, messageSize);
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition,
                        "streamId." + streamId, "last_message_size_in_bytes"),
                        Level.INFO, () -> messageSizeMap.get(topic).get(partition));
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Per.Partition.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition, "last_message_size_in_bytes"),
                        Level.INFO, () -> messageSizeMap.get(topic).get(partition));
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.%s.%s",
                        siddhiAppName, topic, "last_message_size_in_bytes"),
                        Level.INFO, () -> messageSizeMap.get(topic).get(partition));
    }

    //Latency is millis
    public void getLastMessageAckLatency(String topic, int partition, String streamId, long latency) {
        updateLatencyMap(topic, partition, latency);
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition,
                        "streamId." + streamId, "last_message_latency_in_millis"),
                        Level.INFO, () -> latencyMap.get(topic).get(partition));
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Per.Partition.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition, "last_message_latency_in_millis"),
                        Level.INFO, () -> latencyMap.get(topic).get(partition));
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.%s.%s",
                        siddhiAppName, topic, "last_message_latency_in_millis"),
                        Level.INFO, () -> latencyMap.get(topic).get(partition));
    }

    public void getLastCommittedOffset(String topic, int partition, String streamId, long offset) {
        updateOffsetMap(topic, partition, offset);
        MetricsDataHolder.getInstance().getMetricService()
                    .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Current.Offset.%s.%s.%s",
                            siddhiAppName, topic, "partition." + partition, "streamId." + streamId), Level.INFO,
                            () -> offsetMap.get(topic).get(partition));
    }

    public void getLastMessagePublishedTime(String topic, int partition, String streamId, long pushedTimestamp) {
        updateLastMessagePublishedTimeMap(topic, partition, pushedTimestamp);
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition,
                        "streamId." + streamId, "last_message_published_at"),
                        Level.INFO, System::currentTimeMillis);
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.Per.Partition.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition, "last_message_published_at"),
                        Level.INFO, System::currentTimeMillis);
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Sink.%s.%s",
                        siddhiAppName, topic, "last_message_published_at"),
                        Level.INFO, System::currentTimeMillis);
    }

    private void updateOffsetMap(String topic, int partition, long offset) {
        Map<Integer, Long> partitionMap;
        if (offsetMap.get(topic) == null) {
            partitionMap = new ConcurrentHashMap();
        } else {
            partitionMap = offsetMap.get(topic);
        }
        partitionMap.put(partition, offset);
        offsetMap.put(topic, partitionMap);
    }

    private void updateLatencyMap(String topic, int partition, long latency) {
        Map<Integer, Long> partitionMap;
        if (latencyMap.get(topic) == null) {
            partitionMap = new ConcurrentHashMap();
        } else {
            partitionMap = latencyMap.get(topic);
        }
        partitionMap.put(partition, latency);
        latencyMap.put(topic, partitionMap);
    }

    private void updateMessageSizeMap(String topic, int partition, double messageSize) {
        Map<Integer, Double> partitionMap;
        if (messageSizeMap.get(topic) == null) {
            partitionMap = new ConcurrentHashMap();
        } else {
            partitionMap = messageSizeMap.get(topic);
        }
        partitionMap.put(partition, messageSize);
        messageSizeMap.put(topic, partitionMap);
    }

    private void updateLastMessagePublishedTimeMap(String topic, int partition, long pushedTimestamp) {
        Map<Integer, Long> partitionMap;
        if (lastMessagePublishedTimeMap.get(topic) == null) {
            partitionMap = new ConcurrentHashMap();
        } else {
            partitionMap = lastMessagePublishedTimeMap.get(topic);
        }
        partitionMap.put(partition, pushedTimestamp);
        lastMessagePublishedTimeMap.put(topic, partitionMap);
    }
}
