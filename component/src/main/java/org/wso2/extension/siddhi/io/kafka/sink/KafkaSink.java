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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class implements a Kafka sink to publish Siddhi events to a kafka cluster.
 */
@Extension(
        name = "kafka",
        namespace = "sink",
        description = "The Kafka Sink publishes records to a topic with a partition for a Kafka cluster which are in "
                + "format such as `text`, `XML` and `JSON`.\n"
                + "The Kafka Sink will create the default partition for a given topic, if the topic is not already "
                + "been created in the Kafka cluster. The publishing topic and partition can be a dynamic value taken"
                + " from the Siddhi event",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                           description = "This should contain the kafka server list which the kafka sink should be "
                                   + "publishing to. This should be given in comma separated values. "
                                   + "eg: 'localhost:9092,localhost:9093' ",
                           type = {DataType.STRING}),
                @Parameter(name = "topic",
                           description = "The topic list which the sink  should publish to. Only one topic should be "
                                   + "given",
                           type = {DataType.STRING}),
                @Parameter(name = "partition.no",
                           description = "The partition number for the given topic. Only one partition id can be "
                                   + "defined. If this is not defined, the sink will be publishing to the topic's "
                                   + "default partition.",
                           type = {DataType.INT},
                           optional = true,
                           defaultValue = "0"),
                @Parameter(name = "optional.configuration",
                           description = "This may contain all the other possible configurations which the consumer "
                                   + "should be created with."
                                   + "eg: producer.type:async,batch.size:200",
                           optional = true,
                           type = {DataType.STRING},
                           defaultValue = "null")
        },
        examples = {
                @Example(
                        description = "The following query will publish to 'topic_with_partitions' and to its 0th "
                                + "partition",
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@sink("
                                    + "type='kafka', "
                                    + "topic='topic_with_partitions', "
                                    + "partition.no='0', "
                                    + "bootstrap.servers='localhost:9092', "
                                    + "@map(type='xml'))" +
                                "Define stream BarStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n"),
                @Example(
                        description = "The following query will publish dynamic topic and partitions which will be "
                                + "taken from the siddhi event. partition number value will be taken from the "
                                + "'volume' attribute and the topic value will be taken from the 'symbol' attribute.",
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@sink("
                                    + "type='kafka', "
                                    + "topic='{{symbol}}', "
                                    + "partition.no='{{volume}}', "
                                    + "bootstrap.servers='localhost:9092', "
                                    + "@map(type='xml'))" +
                                "Define stream BarStream (symbol string, price float, volume long); \n" +
                                "from FooStream select symbol, price, volume insert into BarStream; \n")}
)
public class KafkaSink extends Sink {

    private ScheduledExecutorService executorService;
    private Producer<String, String>  producer;
    private Option topicOption = null;
    private String bootstrapServers;
    private String optionalConfigs;
    private Option partitionOption;

    private static final String KAFKA_PUBLISH_TOPIC = "topic";
    private static final String KAFKA_BROKER_LIST = "bootstrap.servers";
    private static final String KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    private static final String HEADER_SEPARATOR = ",";
    private static final String ENTRY_SEPARATOR = ":";
    private static final String KAFKA_PARTITION_NO = "partition.no";

    private static final Logger LOG = Logger.getLogger(KafkaSink.class);

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        bootstrapServers = optionHolder.validateAndGetStaticValue(KAFKA_BROKER_LIST);
        optionalConfigs = optionHolder.validateAndGetStaticValue(KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        topicOption = optionHolder.validateAndGetOption(KAFKA_PUBLISH_TOPIC);
        partitionOption = optionHolder.getOrCreateOption(KAFKA_PARTITION_NO, null);
        executorService = siddhiAppContext.getScheduledExecutorService();
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

        if (optionalConfigs != null && optionalConfigs.isEmpty()) {
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
        producer = new KafkaProducer<>(props);
        LOG.info("Kafka producer created.");
    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions) throws ConnectionUnavailableException {
        String topic = topicOption.getValue(transportOptions);
        String partitionNo = partitionOption.getValue(transportOptions);
        try {
            if (null == partitionNo) {
                producer.send(new ProducerRecord<>(topic, payload.toString()));
            } else {
                producer.send(new ProducerRecord<>(topic, partitionNo, payload.toString()));
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
        return new String[]{KAFKA_PUBLISH_TOPIC, KAFKA_PARTITION_NO};
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
