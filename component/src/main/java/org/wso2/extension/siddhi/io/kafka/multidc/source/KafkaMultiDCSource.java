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

package org.wso2.extension.siddhi.io.kafka.multidc.source;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.kafka.sink.KafkaSink;
import org.wso2.extension.siddhi.io.kafka.source.KafkaSource;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This class implements a Kafka multi data center source to receives events from a kafka cluster.
 */
@Extension(
        name = "kafkaMultiDC",
        namespace = "source",
        description = "The Kafka Multi Data Center(DC) Source receives records from the same topic in brokers " +
                "deployed in two different kafka cluster. It will filter out all duplicate messages and try to ensure" +
                "that the events are received in the correct order by using sequence numbers. events are received in" +
                " format such as `text`, `XML` and `JSON`.The Kafka Source will create the default partition  " +
                "'0' for a given topic, if the topic is not already been created in the Kafka cluster.",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                        description = "This should contain the kafka server list which the kafka source should be "
                                + "listening to. This should be given in comma separated values. "
                                + "eg: 'localhost:9092,localhost:9093' ",
                        type = {DataType.STRING}),
                @Parameter(name = "topic",
                        description = "The topic  which the source would be listening to. eg: 'topic_one' ",
                        type = {DataType.STRING}),
                @Parameter(name = "partition.no",
                        description = "The partition number for the given topic",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "0"),
                @Parameter(name = "optional.configuration",
                        description = "This may contain all the other possible configurations which the consumer "
                                + "should be created with."
                                + "eg: producer.type:async,batch.size:200",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null")
        },
        examples = {
                @Example(
                        description = "The following query will listen to 'kafka_topic' topic deployed in broker "
                                + "host1:9092 and host1:9093 with partition 1. There will be a thread created for " +
                                "each broker. The receiving xml events will be mapped to a siddhi event "
                                + "and will be send to the FooStream.",
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
                                "from FooStream select symbol, price, volume insert into BarStream;\n")
        }
)
public class KafkaMultiDCSource extends Source {
    private static final String KAFKA_TOPIC = "topic";
    private static final String KAFKA_PARTITION_NO = "partition.no";
    private static final Logger LOG = Logger.getLogger(KafkaMultiDCSource.class);
    private static final String LAST_RECEIVED_SEQ_NO_KEY = "lastConsumedSeqNo";
    private SourceEventListener eventListener;
    private Map<String, KafkaSource> sources = new HashMap<>();
    private String[] bootstrapServers;
    private SourceSynchronizer synchronizer;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] strings,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.eventListener = sourceEventListener;
        String serverList = optionHolder.validateAndGetStaticValue(KafkaSource
            .ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS);
        bootstrapServers = serverList.split(",");
        if (bootstrapServers.length != 2) {
            //TODO : add more contect
            throw new SiddhiAppValidationException("There should be two servers listed in " +
                    "'bootstrap.servers' configuration");
        }
        synchronizer = new SourceSynchronizer(sourceEventListener, bootstrapServers, 1000, 10);
        LOG.info("Initializing kafka source for bootstrap server :" + bootstrapServers[0]);
        Interceptor interceptor = new Interceptor(bootstrapServers[0], synchronizer);
        OptionHolder options = createOptionHolders(bootstrapServers[0], optionHolder);
        KafkaSource source = new KafkaSource();
        source.init(interceptor, options, strings, configReader, siddhiAppContext);
        sources.put(bootstrapServers[0], source);

        LOG.info("Initializing kafka source for bootstrap server :" + bootstrapServers[1]);
        interceptor = new Interceptor(bootstrapServers[1], synchronizer);
        options = createOptionHolders(bootstrapServers[1], optionHolder);
        source = new KafkaSource();
        source.init(interceptor, options, strings, configReader, siddhiAppContext);
        sources.put(bootstrapServers[1], source);
    }


    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        StringBuilder errorMessage = new StringBuilder();
        for (Map.Entry entry: sources.entrySet()) {
            try {
                ((KafkaSource) entry.getValue()).connect(connectionCallback);
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

    @Override
    public Map<String, Object> currentState() {
        HashMap<String, Object> state = new HashMap<>();
        for (Map.Entry<String, KafkaSource> entry: sources.entrySet()) {
            state.put(entry.getKey(), entry.getValue().currentState());
        }
        state.put(LAST_RECEIVED_SEQ_NO_KEY, synchronizer.getLastConsumedSeqNo());
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronizer.setLastConsumedSeqNo((Long) map.get(LAST_RECEIVED_SEQ_NO_KEY));

        Map<String, Object> sourceState = (Map<String, Object>) map.get(bootstrapServers[0]);
        sources.get(bootstrapServers[0]).restoreState(sourceState);

        sourceState = (Map<String, Object>) map.get(bootstrapServers[1]);
        sources.get(bootstrapServers[1]).restoreState(sourceState);
    }

    // Create option holders for two sources to connect to two bootstrap servers
    private  OptionHolder createOptionHolders(String server, OptionHolder originalOptionHolder) {
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

        Extension extension = KafkaSource.class.getAnnotation(org.wso2.siddhi.annotation.Extension.class);

        OptionHolder holder = new OptionHolder(eventListener.getStreamDefinition(), options, new HashMap<>(),
                extension);

        return holder;
    }
}


class Interceptor implements SourceEventListener {
    private static final Logger LOG = Logger.getLogger(Interceptor.class);
    private String sourceId;
    private SourceSynchronizer synchronizer;

    public Interceptor(String sourceId, SourceSynchronizer synchronizer) {
        this.sourceId = sourceId;
        this.synchronizer = synchronizer;
    }
    
    @Override
    public void onEvent(Object event, String[] strings) {
        String eventString = (String) event;
        int headerStartingIndex = eventString.indexOf(KafkaSink.SEQ_NO_HEADER_DELIMITER);
        if (headerStartingIndex > 0) {
            String eventBody = eventString.substring(headerStartingIndex + 1);
            String header = eventString.substring(0, headerStartingIndex);

            String[] headerElements = header.split(KafkaSink.SEQ_NO_HEADER_FIELD_SEPERATOR);
            Integer seqNo = Integer.parseInt(headerElements[1]);
            synchronizer.onEvent(sourceId, seqNo, eventBody, strings);
        } else {
            LOG.warn("Sequence number is not contained in the message. Dropping the message :"
                + eventString);
        }
    }

    @Override
    public StreamDefinition getStreamDefinition() {
        return null;
    }
}





