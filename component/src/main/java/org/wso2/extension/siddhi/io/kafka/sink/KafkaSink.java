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

package org.wso2.extension.siddhi.io.kafka.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements a Kafka sink to publish Siddhi events to a kafka cluster.
 */
@Extension(
        name = "kafka",
        namespace = "sink",
        description = "A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka " +
                "cluster. The events can be published in the `TEXT` `XML` or `JSON` format.\n" +
                "If the topic is not already created in the Kafka cluster, the Kafka sink creates the default " +
                "partition for the given topic. The publishing topic and partition can be a dynamic value taken " +
                "from the Siddhi event.\n" +
                "To configure a sink to use the Kafka transport, the `type` parameter should have `kafka` as its " +
                "value.",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                           description = " This parameter specifies the list of Kafka servers to which the Kafka " +
                                   "sink must publish events. This list should be provided as a set of comma " +
                                   "separated values. e.g., `localhost:9092,localhost:9093`.",
                           type = {DataType.STRING}),
                @Parameter(name = "topic",
                           description = "The topic to which the Kafka sink needs to publish events. Only one " +
                                   "topic must be specified.",
                           type = {DataType.STRING}),
                @Parameter(name = "partition.no",
                           description = "The partition number for the given topic. Only one partition ID can be " +
                                   "defined. If no value is specified for this parameter, the Kafka sink publishes " +
                                   "to the default partition of the topic (i.e., 0)",
                           type = {DataType.INT},
                           optional = true,
                           defaultValue = "0"),
                @Parameter(name = "sequence.id",
                        description = "A unique identifier to identify the messages published by this sink. This ID " +
                                "allows receivers to identify the sink that published a specific message.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "key",
                           description = "The key contains the values that are used to maintain ordering in a Kafka" +
                                   " partition.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "null"),
                @Parameter(name = "optional.configuration",
                           description = "This parameter contains all the other possible configurations that the " +
                                   "producer is created with. \n" +
                                   "e.g., `producer.type:async,batch.size:200`",
                           optional = true,
                           type = {DataType.STRING},
                           defaultValue = "null")
        },
        examples = {
                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@sink(\n" +
                                "type='kafka',\n" +
                                "topic='topic_with_partitions',\n" +
                                "partition.no='0',\n" +
                                "bootstrap.servers='localhost:9092',\n" +
                                "@map(type='xml'))\n" +
                                "Define stream BarStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n",
                        description = "This Kafka sink configuration publishes to 0th partition of the topic named " +
                        "`topic_with_partitions`."),

                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@sink(\n" +
                                "type='kafka',\n" +
                                "topic='{{symbol}}',\n" +
                                "partition.no='{{volume}}',\n" +
                                "bootstrap.servers='localhost:9092',\n" +
                                "@map(type='xml'))\n" +
                                "Define stream BarStream (symbol string, price float, volume long); \n" +
                                "from FooStream select symbol, price, volume insert into BarStream;",
                        description = "This query publishes dynamic topic and partitions that are taken from the " +
                        "Siddhi event. The value for `partition.no` is taken from the `volume` attribute, and the " +
                        "topic value is taken from the `symbol` attribute.")
        }
)
public class KafkaSink extends Sink {

    private Producer<String, String>  producer;
    protected Option topicOption = null;
    protected String bootstrapServers;
    protected String optionalConfigs;
    protected Option partitionOption;
    protected Boolean isSequenced = false;
    protected AtomicInteger lastSentSequenceNo = new AtomicInteger(0);
    protected String sequenceId = null;

    public static final String LAST_SENT_SEQ_NO_PERSIST_KEY = "lastSentSequenceNo";
    public static final String SEQ_NO_HEADER_DELIMITER = "~";
    public static final String SEQ_NO_HEADER_FIELD_SEPERATOR = ":";
    protected Option keyOption;

    protected static final String KAFKA_PUBLISH_TOPIC = "topic";
    protected static final String KAFKA_BROKER_LIST = "bootstrap.servers";
    protected static final String KAFKA_MESSAGE_KEY = "key";
    protected static final String KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    protected static final String HEADER_SEPARATOR = ",";
    protected static final String ENTRY_SEPARATOR = ":";
    protected static final String KAFKA_PARTITION_NO = "partition.no";
    protected static final String SEQ_ID = "sequence.id";

    private static final Logger LOG = Logger.getLogger(KafkaSink.class);

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        bootstrapServers = optionHolder.validateAndGetStaticValue(KAFKA_BROKER_LIST);
        optionalConfigs = optionHolder.validateAndGetStaticValue(KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        topicOption = optionHolder.validateAndGetOption(KAFKA_PUBLISH_TOPIC);
        partitionOption = optionHolder.getOrCreateOption(KAFKA_PARTITION_NO, null);
        sequenceId = optionHolder.validateAndGetStaticValue(SEQ_ID, null);
        isSequenced = sequenceId != null;
        keyOption = optionHolder.getOrCreateOption(KAFKA_MESSAGE_KEY, null);
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        readOptionalConfigs(props, optionalConfigs);

        producer = new KafkaProducer<>(props);
        LOG.info("Kafka producer created.");
    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions) throws ConnectionUnavailableException {
        String topic = topicOption.getValue(transportOptions);
        String partitionNo = partitionOption.getValue(transportOptions);
        String key = keyOption.getValue(transportOptions);
        try {
            String payloadToSend;
            if (isSequenced) {
                StringBuilder strPayload = new StringBuilder();
                strPayload.append(sequenceId).append(SEQ_NO_HEADER_FIELD_SEPERATOR).append(lastSentSequenceNo)
                        .append(SEQ_NO_HEADER_DELIMITER)
                        .append(payload.toString());
                payloadToSend = strPayload.toString();
                lastSentSequenceNo.incrementAndGet();
            } else {
                payloadToSend = payload.toString();
            }

            if (null == partitionNo) {
                producer.send(new ProducerRecord<>(topic, null, key, payloadToSend));
            } else {
                producer.send(new ProducerRecord<>(topic, Integer.parseInt(partitionNo), key, payloadToSend));
            }
        } catch (Exception e) {
            LOG.error(String.format("Failed to publish the message to [topic] %s [partition-no] %s. Error: %s",
                    topic, partitionNo, e.getMessage()), e);
        }
    }

    @Override
    public void disconnect() {
        //close producer
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void destroy() {
        //not required
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{KAFKA_PUBLISH_TOPIC, KAFKA_PARTITION_NO, KAFKA_MESSAGE_KEY};
    }

    @Override
    public Map<String, Object> currentState() {
        if (isSequenced) {
            Map<String, Object> state = new HashMap<>();
            state.put(LAST_SENT_SEQ_NO_PERSIST_KEY, lastSentSequenceNo.get());
            return state;
        } else {
            return null;
        }
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        if (isSequenced) {
            Object sequenceNumber = state.get(LAST_SENT_SEQ_NO_PERSIST_KEY);
            if (sequenceNumber != null) {
                lastSentSequenceNo.set((Integer) sequenceNumber);
            }
        }
    }

    public static void readOptionalConfigs(Properties props, String optionalConfigs) {
        if (optionalConfigs != null && !optionalConfigs.isEmpty()) {
            String[] optionalProperties = optionalConfigs.split(HEADER_SEPARATOR);
            if (optionalProperties.length > 0) {
                for (String header : optionalProperties) {
                    try {
                        String[] configPropertyWithValue = header.split(ENTRY_SEPARATOR, 2);
                        props.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                    } catch (Exception e) {
                        LOG.warn("Optional property '" + header + "' is not defined in the correct format.", e);
                    }
                }
            }
        }
    }
}
