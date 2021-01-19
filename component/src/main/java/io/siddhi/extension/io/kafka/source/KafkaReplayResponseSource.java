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
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
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
        description = "This source is used to listen to replayed events requested from kafka-replay-request sink",
        parameters = {
                @Parameter(
                        name = "sink.id",
                        description = "a unique SINK_ID that should be set for each grpc-call source. There is a 1:1 " +
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
    public StateFactory<KafkaSourceState> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                               String[] requiredProperties, ConfigReader configReader,
                                               SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.optionHolder = optionHolder;
        this.requiredProperties = requiredProperties.clone();
        this.sourceEventListener = sourceEventListener;
        if (configReader != null) {
            bootstrapServers = configReader.readConfig(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS,
                    optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS));
            groupID = configReader.readConfig(ADAPTOR_SUBSCRIBER_GROUP_ID,
                    optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_GROUP_ID));
            threadingOption = configReader.readConfig(THREADING_OPTION,
                    optionHolder.validateAndGetStaticValue(THREADING_OPTION));
            seqEnabled = configReader.readConfig(SEQ_ENABLED,
                    optionHolder.validateAndGetStaticValue(SEQ_ENABLED, "false"))
                    .equalsIgnoreCase("true");
            optionalConfigs = configReader.readConfig(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES,
                    optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null));
            isBinaryMessage = Boolean.parseBoolean(configReader.readConfig(IS_BINARY_MESSAGE,
                    optionHolder.validateAndGetStaticValue(IS_BINARY_MESSAGE, "false")));
            enableOffsetCommit = Boolean.parseBoolean(configReader.readConfig(ADAPTOR_ENABLE_OFFSET_COMMIT,
                    optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_OFFSET_COMMIT, "true")));
            enableAsyncCommit = Boolean.parseBoolean(configReader.readConfig(ADAPTOR_ENABLE_ASYNC_COMMIT,
                    optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_ASYNC_COMMIT, "true")));

        } else {
            bootstrapServers = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS);
            groupID = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_GROUP_ID);
            threadingOption = optionHolder.validateAndGetStaticValue(THREADING_OPTION);
            optionalConfigs = optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
            isBinaryMessage = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(IS_BINARY_MESSAGE,
                    "false"));
            enableOffsetCommit = Boolean.parseBoolean(optionHolder.
                    validateAndGetStaticValue(ADAPTOR_ENABLE_OFFSET_COMMIT, "true"));
            enableAsyncCommit = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_ASYNC_COMMIT,
                    "true"));
        }
        seqEnabled = optionHolder.validateAndGetStaticValue(SEQ_ENABLED, "false").equalsIgnoreCase("true");
        optionalConfigs = optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        isBinaryMessage = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(IS_BINARY_MESSAGE,
                "false"));
        enableOffsetCommit = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_OFFSET_COMMIT,
                "true"));
        enableAsyncCommit = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_ASYNC_COMMIT,
                "true"));
        this.sinkId = optionHolder.validateAndGetStaticValue(Constants.SINK_ID);
        KafkaReplayResponseSourceRegistry.getInstance().putKafkaReplayResponseSource(sinkId, this);
        return () -> new KafkaSourceState(seqEnabled);
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, KafkaSourceState kafkaSourceState) {
    }

    public void onReplayRequest(String partitionForReplay, String startOffset, String endOffset, String replayTopic)
            throws ConnectionUnavailableException {
        try {
            String[] partitionAsListForReplay = new String[]{partitionForReplay};
            ExecutorService executorService = siddhiAppContext.getExecutorService();
            KafkaReplayThread kafkaReplayThread =
                    new KafkaReplayThread(sourceEventListener, new String[]{replayTopic}, partitionAsListForReplay,
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
