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
package io.siddhi.extension.io.kafka.source;


/**
 * This class implements a Kafka source to receive events from a kafka cluster.
 */

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.kafka.Constants;
import io.siddhi.extension.io.kafka.util.KafkaReplayResponseSourceRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * aerer
 */
@Extension(
        name = "kafka-replay-response",
        namespace = "source",
        description = "sdfsdf",
        parameters = {
                @Parameter(
                        name = "sink.id",
                        description = "a unique ID that should be set for each grpc-call source. There is a 1:1 " +
                                "mapping between grpc-call sinks and grpc-call-response sources. Each sink has one " +
                                "particular source listening to the responses to requests published from that sink. " +
                                "So the same sink.id should be given when writing the sink also.",
                        type = {DataType.INT}),
        },
        examples = {
                @Example(
                        syntax = "sdfsdf",
                        description = "sdfsdf")
        }
)
public class KafkaReplayResponseSource extends KafkaSource {
    private String sinkId;
    private List<Future<?>> futureList = new ArrayList<>();
    private List<KafkaReplayThread> kafkaReplayThreadList = new ArrayList<>();

    @Override
    public void connect(ConnectionCallback connectionCallback, KafkaSourceState kafkaSourceState) {
    }

    @Override
    public void setSinkId(OptionHolder optionHolder) {
        this.sinkId = optionHolder.validateAndGetStaticValue(Constants.ID);
        KafkaReplayResponseSourceRegistry.getInstance().putKafkaReplayResponseSource(sinkId, this);
    }

    public void onReplayRequest(String startOffset, String endOffset) throws ConnectionUnavailableException {
        try {
            ExecutorService executorService = siddhiAppContext.getExecutorService();
            KafkaReplayThread kafkaReplayThread =
                    new KafkaReplayThread(sourceEventListener, topics, partitions,
                            KafkaSource.createConsumerConfig(bootstrapServers, groupID, optionalConfigs,
                                    isBinaryMessage, enableOffsetCommit), false, isBinaryMessage, enableOffsetCommit,
                            enableAsyncCommit, requiredProperties, Integer.parseInt(startOffset),
                            Integer.parseInt(endOffset), futureList.size(), sinkId);
            kafkaReplayThreadList.add(kafkaReplayThread);
            futureList.add(executorService.submit(kafkaReplayThread));
        } catch (SiddhiAppRuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConnectionUnavailableException("Error when initiating connection with Kafka server: " +
                    bootstrapServers + " in Siddhi App: " + siddhiAppContext.getName(), e);
        }
    }

    public void onReplayFinish(int threadId) {
        kafkaReplayThreadList.get(threadId).shutdownConsumer();
        Future future = futureList.get(threadId);
        if (!future.isCancelled()) {
            future.cancel(true);
        }
    }
}
