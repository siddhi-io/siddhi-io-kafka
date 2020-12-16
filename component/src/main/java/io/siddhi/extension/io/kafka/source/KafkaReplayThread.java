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
import io.siddhi.extension.io.kafka.util.KafkaReplayResponseSourceRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * This runnable processes each Kafka message and sends it to siddhi.
 */
public class KafkaReplayThread extends KafkaConsumerThread {
    private int startOffset;
    private int endOffset;
    private int threadId;
    private String sinkId;

    KafkaReplayThread(SourceEventListener sourceEventListener, String[] topics, String[] partitions, Properties props,
                      boolean isPartitionWiseThreading, boolean isBinaryMessage, boolean enableOffsetCommit,
                      boolean enableAsyncCommit, String[] requiredProperties, int startOffset, int endOffset,
                      int threadId, String sinkId) {
        super(sourceEventListener, topics, partitions, props, isPartitionWiseThreading, isBinaryMessage,
                enableOffsetCommit, enableAsyncCommit, requiredProperties);
        this.threadId = threadId;
        this.sinkId = sinkId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.isReplayThread = true;
    }

    @Override
    void seekToRequiredOffset() {
        consumer.seekToBeginning(partitionsList); // TODO: 2020-12-11 seek to start offset
    }

    @Override
    boolean isRecordAfterStartOffset(ConsumerRecord record) {
        return record.offset() >= startOffset;
    }

    @Override
    boolean endReplay(ConsumerRecord record) {
        if (record.offset() >= endOffset) {
            KafkaReplayResponseSourceRegistry.getInstance().getKafkaReplayResponseSource(sinkId)
                    .onReplayFinish(threadId);
            return true;
        } else {
            return false;
        }
    }
}
