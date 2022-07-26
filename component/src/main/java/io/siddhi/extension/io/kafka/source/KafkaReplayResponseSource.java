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
 * This source is used to listen to replayed events requested from kafka-replay-request sink
 */
@Extension(
        name = "kafka-replay-response",
        namespace = "source",
        description = "This source is used to listen to replayed events requested from kafka-replay-request sink",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                        description = "This specifies the list of Kafka servers to which the Kafka source " +
                                "must listen. This list can be provided as a set of comma-separated values.\n" +
                                "e.g., `localhost:9092,localhost:9093`",
                        type = {DataType.STRING}),
                @Parameter(name = "group.id",
                        description = "This is an ID to identify the Kafka source group. The group ID ensures " +
                                "that sources with the same topic and partition that are in the same group do not" +
                                " receive the same event.",
                        type = {DataType.STRING}),
                @Parameter(name = "threading.option",
                        description = " This specifies whether the Kafka source is to be run on a single thread," +
                                " or in multiple threads based on a condition. Possible values are as follows:\n" +
                                "`single.thread`: To run the Kafka source on a single thread.\n" +
                                "`topic.wise`: To use a separate thread per topic.\n" +
                                "`partition.wise`: To use a separate thread per partition.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "sink.id",
                        description = "a unique SINK_ID .",
                        type = {DataType.INT}),
        },
        examples = {
                @Example(
                        syntax = "@App:name('TestKafkaReplay')\n" +
                                "\n" +
                                "@sink(type='kafka-replay-request', sink.id='1')\n" +
                                "define stream BarStream (topicForReplay string, partitionForReplay string, " +
                                "startOffset string, endOffset string);\n" +
                                "\n" +
                                "@info(name = 'query1')\n" +
                                "@source(type='kafka-replay-response',  group.id='group', threading.option=" +
                                "'single.thread', bootstrap.servers='localhost:9092', sink.id='1',\n" +
                                "@map(type='json'))\n" +
                                "Define stream FooStream (symbol string, amount double);\n" +
                                "\n" +
                                "@sink(type='log')\n" +
                                "Define stream logStream(symbol string, amount double);\n" +
                                "\n" +
                                "from FooStream select * insert into logStream;",
                        description = "In this app we can send replay request events into BarStream and observe the " +
                                "replayed events in the logStream")
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
                                    isBinaryMessage, enableOffsetCommit),
                            false, isBinaryMessage, enableOffsetCommit,
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
