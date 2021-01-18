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

/**
 * Metric class for Kafka Source
 */
public class SourceMetrics extends Metrics {
    private Map<String, Map<Integer, Long>> topicOffsetMap = null;
    private long consumerLag;

    public SourceMetrics(String siddhiAppName, String streamId) {
        super(siddhiAppName, streamId);
    }

    public Counter getTotalReads() { //to count the total reads from siddhi app level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Reads.%s", siddhiAppName, "kafka"),
                        Level.INFO);
    }

    public Counter getReadCountPerStream(String topic, Integer partition, String groupId) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Source.Reads.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "stream_id." + streamId, "partition." + partition, "groupId." + groupId)
                        , Level.INFO);
    }

    public void getCurrentOffset(String topic, Integer partition, String groupId) {
        if (topicOffsetMap != null) {
            MetricsDataHolder.getInstance().getMetricService()
                    .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Source.Current.Offset.%s.%s.%s.%s",
                            siddhiAppName, topic, "partition." + partition, "groupId." + groupId,
                            "stream_id." + streamId), Level.INFO, () -> topicOffsetMap.get(topic).get(partition));
        }
    }

    public Counter getErrorCountPerStream(String topic, String groupId, String errorString) {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Source.Errors.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "stream_id." + streamId, "groupId." + groupId, "errorString." +
                                errorString), Level.INFO);
    }

    public void getLastMessageConsumedTime(String topic, String groupId) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Source.Per.Stream.%s.%s.%s.%s",
                        siddhiAppName, topic, "groupId." + groupId,
                        "streamId." + streamId, "last_message_consumed_at"),
                        Level.INFO, System::currentTimeMillis);
    }

    public synchronized void getConsumerLag(String topic, String groupId, int partition, long recordTimestamp) {
        setConsumerLag(System.currentTimeMillis() - recordTimestamp);
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Kafka.Source.Per.Stream.%s.%s.%s.%s.%s",
                        siddhiAppName, topic, "partition." + partition, "groupId." + groupId,
                        "streamId." + streamId, "consumer_lag"),
                        Level.INFO, () -> getConsumerLag());
    }

    public void setTopicOffsetMap(Map<String, Map<Integer, Long>> topicOffsetMap) {
        this.topicOffsetMap = topicOffsetMap;
    }

    public long getConsumerLag() {
        return consumerLag;
    }

    public void setConsumerLag(long consumerLag) {
        this.consumerLag = consumerLag;
    }
}



