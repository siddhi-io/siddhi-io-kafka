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

package io.siddhi.extension.io.kafka.multidc.source;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.kafka.sink.KafkaSink;
import io.siddhi.extension.io.kafka.source.KafkaSource;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This class implements a Kafka multi data center source to receives events from a kafka cluster.
 */
@Extension(
        name = "kafkaMultiDC",
        namespace = "source",
        description = "The Kafka Multi-Datacenter(DC) source receives records from the same topic in brokers " +
                "deployed in two different kafka clusters. It filters out all the duplicate messages and ensures" +
                "that the events are received in the correct order using sequential numbering. It receives events in" +
                " formats such as `TEXT`, `XML` JSON` and `Binary`.The Kafka Source creates the default partition" +
                " '0' for a given topic, if the topic has not yet been created in the Kafka cluster.",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                        description = "This contains the kafka server list which the kafka source "
                                + "listens to. This is given using comma-separated values. "
                                + "eg: 'localhost:9092,localhost:9093' ",
                        type = {DataType.STRING}),
                @Parameter(name = "topic",
                        description = "This is the topic that the source listens to. eg: 'topic_one' ",
                        type = {DataType.STRING}),
                @Parameter(name = "partition.no",
                        description = "This is the partition number of the given topic.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "0"),
                @Parameter(name = "is.binary.message",
                        description = "In order to receive the binary events via the Kafka Multi-DC source, the " +
                                "value of this parameter needs to be set to 'True'.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "optional.configuration",
                        description = "This contains all the other possible configurations with which the consumer "
                                + "can be created."
                                + "eg: producer.type:async,batch.size:200",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null")
        },
        examples = {
                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source("
                                + "type='kafkaMultiDC', "
                                + "topic='kafka_topic', "
                                + "bootstrap.servers='host1:9092,host1:9093', "
                                + "partition.no='1', "
                                + "@map(type='xml'))\n" +
                                "Define stream FooStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n",
                        description = "The following query listens to 'kafka_topic' topic, deployed in the broker " +
                                "host1:9092 and host1:9093, with partition 1. A thread is created for " +
                                "each broker. The receiving xml events are mapped to a siddhi event " +
                                "and sent to the FooStream.")
        }
)
public class KafkaMultiDCSource extends Source<KafkaMultiDCSource.KafkaMultiDCSourceState> {
    private static final String KAFKA_TOPIC = "topic";
    private static final String KAFKA_PARTITION_NO = "partition.no";
    private static final Logger LOG = Logger.getLogger(KafkaMultiDCSource.class);
    private static final String LAST_RECEIVED_SEQ_NO_KEY = "lastConsumedSeqNo";
    private SourceEventListener eventListener;
    private Map<String, KafkaSource> sources = new HashMap<>();
    private Map<String, StateFactory<KafkaSource.KafkaSourceState>> stateFactories = new HashMap<>();
    private String[] bootstrapServers;
    private SourceSynchronizer synchronizer;

    @Override
    public StateFactory<KafkaMultiDCSourceState> init(SourceEventListener sourceEventListener,
                                                      OptionHolder optionHolder, String[] transportPropertyNames,
                                                      ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.eventListener = sourceEventListener;
        String serverList = optionHolder.validateAndGetStaticValue(KafkaSource
                .ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS);
        boolean isBinaryMessage = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(KafkaSource.IS_BINARY_MESSAGE, "false"));
        bootstrapServers = serverList.split(",");
        if (bootstrapServers.length != 2) {
            throw new SiddhiAppValidationException("There should be two servers listed in " +
                    "'bootstrap.servers' configuration to ensure fault tolerant kafka messaging.");
        }
        synchronizer = new SourceSynchronizer(sourceEventListener, bootstrapServers, 1000,
                10);
        LOG.info("Initializing kafka source for bootstrap server :" + bootstrapServers[0]);
        Interceptor interceptor = new Interceptor(bootstrapServers[0], synchronizer, isBinaryMessage);
        OptionHolder options = createOptionHolders(bootstrapServers[0], optionHolder);
        KafkaSource source = new KafkaSource();
        stateFactories.put(bootstrapServers[0], source.init(interceptor, options, transportPropertyNames,
                configReader, siddhiAppContext));
        sources.put(bootstrapServers[0], source);

        LOG.info("Initializing kafka source for bootstrap server :" + bootstrapServers[1]);
        interceptor = new Interceptor(bootstrapServers[1], synchronizer, isBinaryMessage);
        options = createOptionHolders(bootstrapServers[1], optionHolder);
        source = new KafkaSource();
        stateFactories.put(bootstrapServers[1], source.init(interceptor, options, transportPropertyNames,
                configReader, siddhiAppContext));
        sources.put(bootstrapServers[1], source);
        return new KafkaMultiDCSourceStateFactory(stateFactories);
    }


    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, KafkaMultiDCSourceState kafkaMultiDCSourceState)
            throws ConnectionUnavailableException {
        StringBuilder errorMessage = new StringBuilder();
        for (Map.Entry entry : sources.entrySet()) {
            try {
                KafkaSource.KafkaSourceState kafkaSourceState = kafkaMultiDCSourceState.kafkaSourceStateMap.
                        get(entry.getKey().toString());
                ((KafkaSource) entry.getValue()).connect((Source.ConnectionCallback) connectionCallback,
                        kafkaSourceState);
                LOG.info("Connect to bootstrap server " + entry.getKey());
            } catch (ConnectionUnavailableException e) {
                errorMessage.append("Error occurred while connecting to ")
                        .append(entry.getKey()).append(":")
                        .append(e.getMessage()).append("\n");
            }
        }

        if (!errorMessage.toString().isEmpty()) {
            LOG.error("Error while trying to connect boot strap server(s): " + errorMessage.toString());
            throw new ConnectionUnavailableException(errorMessage.toString());
        }
    }

    @Override
    public void disconnect() {
        sources.values().forEach(KafkaSource::disconnect);

    }

    @Override
    public void destroy() {
        sources.values().forEach(KafkaSource::destroy);
    }

    @Override
    public void pause() {
        sources.values().forEach(KafkaSource::pause);
    }

    @Override
    public void resume() {
        sources.values().forEach(KafkaSource::resume);
    }

    // Create option holders for two sources to connect to two bootstrap servers
    private OptionHolder createOptionHolders(String server, OptionHolder originalOptionHolder) {
        Map<String, String> options = new HashMap<>();

        options.put(KafkaSource.ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS, server);
        options.put(KafkaSource.ADAPTOR_SUBSCRIBER_GROUP_ID, UUID.randomUUID().toString());
        options.put(KafkaSource.THREADING_OPTION, KafkaSource.SINGLE_THREADED);
        options.put(KafkaSource.SEQ_ENABLED, "false");

        String partition = originalOptionHolder.validateAndGetStaticValue(KAFKA_PARTITION_NO, "0");
        options.put(KafkaSource.ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST, partition);

        String topic = originalOptionHolder.validateAndGetStaticValue(KAFKA_TOPIC);
        options.put(KafkaSource.ADAPTOR_SUBSCRIBER_TOPIC, topic);

        String optionalConfigs = originalOptionHolder.validateAndGetStaticValue(
                KafkaSource.ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        options.put(KafkaSource.ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, optionalConfigs);

        String isBinaryMessage = originalOptionHolder.validateAndGetStaticValue(KafkaSource.IS_BINARY_MESSAGE,
                "false");
        options.put(KafkaSource.IS_BINARY_MESSAGE, isBinaryMessage);

        Extension extension = KafkaSource.class.getAnnotation(io.siddhi.annotation.Extension.class);

        OptionHolder holder = new OptionHolder(eventListener.getStreamDefinition(), options, new HashMap<>(),
                extension);

        return holder;
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * State class for Kafka MultiDC source.
     */
    class KafkaMultiDCSourceState extends State {
        Map<String, KafkaSource.KafkaSourceState> kafkaSourceStateMap = new HashMap<>();

        KafkaMultiDCSourceState(Map<String, KafkaSource.KafkaSourceState> kafkaSourceStateMap) {
            this.kafkaSourceStateMap = kafkaSourceStateMap;
        }

        @Override
        public Map<String, Object> snapshot() {
            HashMap<String, Object> state = new HashMap<>();
            for (Map.Entry<String, KafkaSource.KafkaSourceState> entry : kafkaSourceStateMap.entrySet()) {
                state.put(entry.getKey(), entry.getValue().snapshot());
            }
            state.put(LAST_RECEIVED_SEQ_NO_KEY, synchronizer.getLastConsumedSeqNo());
            return state;
        }

        public void restore(Map<String, Object> state) {
            synchronizer.setLastConsumedSeqNo((Long) state.get(LAST_RECEIVED_SEQ_NO_KEY));
            for (Map.Entry<String, KafkaSource.KafkaSourceState> entry : kafkaSourceStateMap.entrySet()) {
                entry.getValue().restore((Map<String, Object>) state.get(entry.getKey()));
            }
            kafkaSourceStateMap = (Map<String, KafkaSource.KafkaSourceState>) state.get("SOURCE_STATES");
        }

        @Override
        public boolean canDestroy() {
            return false;
        }
    }

    class KafkaMultiDCSourceStateFactory implements StateFactory<KafkaMultiDCSource.KafkaMultiDCSourceState> {

        private Map<String, StateFactory<KafkaSource.KafkaSourceState>> stateFactories;

        public KafkaMultiDCSourceStateFactory(
                Map<String, StateFactory<KafkaSource.KafkaSourceState>> kafkaStateFactories) {
            this.stateFactories = kafkaStateFactories;
        }

        @Override
        public KafkaMultiDCSource.KafkaMultiDCSourceState createNewState() {
            Map<String, KafkaSource.KafkaSourceState> kafkaSourceStateMap = new HashMap<>();
            for (Map.Entry<String, StateFactory<KafkaSource.KafkaSourceState>> entry : stateFactories.entrySet()) {
                String sourceKey = entry.getKey();
                StateFactory<KafkaSource.KafkaSourceState> kafkaSourceStateStateFactory = entry.getValue();
                kafkaSourceStateMap.put(sourceKey, kafkaSourceStateStateFactory.createNewState());
            }
            return new KafkaMultiDCSourceState(kafkaSourceStateMap);
        }
    }
}


class Interceptor implements SourceEventListener {
    private static final Logger LOG = Logger.getLogger(Interceptor.class);
    private String sourceId;
    private SourceSynchronizer synchronizer;
    private boolean isBinaryMessage;

    public Interceptor(String sourceId, SourceSynchronizer synchronizer, boolean isBinaryMessage) {
        this.sourceId = sourceId;
        this.synchronizer = synchronizer;
        this.isBinaryMessage = isBinaryMessage;
    }

    @Override
    public void onEvent(Object event, String[] transportProperties) {
        onEventReceive(event, transportProperties, null);
    }

    @Override
    public void onEvent(Object event, String[] transportProperties, String[] transportSyncProperties) {
        onEventReceive(event, transportProperties, transportSyncProperties);
    }

    @Override
    public StreamDefinition getStreamDefinition() {
        return null;
    }

    private void onEventReceive(Object event, String[] transportProperties, String[] transportSyncProperties) {
        if (!isBinaryMessage) {
            String eventString = (String) event;
            int headerStartingIndex = eventString.indexOf(KafkaSink.SEQ_NO_HEADER_DELIMITER);
            if (headerStartingIndex > 0) {
                String eventBody = eventString.substring(headerStartingIndex + 1);
                String header = eventString.substring(0, headerStartingIndex);

                String[] headerElements = header.split(KafkaSink.SEQ_NO_HEADER_FIELD_SEPERATOR);
                Integer seqNo = Integer.parseInt(headerElements[1]);
                synchronizer.onEvent(sourceId, seqNo, eventBody, transportProperties);
            } else {
                LOG.warn("Sequence number is not contained in the message. Dropping the message :" + eventString);
            }
        } else {
            byte[] byteEvents = (byte[]) event;
            int stringSize = ByteBuffer.wrap(byteEvents).getInt();
            String header = new String(byteEvents, 4, stringSize - 1, Charset.defaultCharset());
            if (!header.isEmpty()) {
                String[] headerElements = header.split(KafkaSink.SEQ_NO_HEADER_FIELD_SEPERATOR);
                Integer seqNo = Integer.parseInt(headerElements[1]);
                byte[] eventBody = Arrays.copyOfRange(byteEvents, stringSize + 4,
                        byteEvents.length);
                synchronizer.onEvent(sourceId, seqNo, eventBody, transportProperties);
            } else {
                LOG.warn("Sequence number is not contained in the message. Dropping the message");
            }
        }
    }
}

