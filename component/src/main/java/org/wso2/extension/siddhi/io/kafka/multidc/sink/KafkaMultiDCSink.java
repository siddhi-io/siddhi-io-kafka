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

package org.wso2.extension.siddhi.io.kafka.multidc.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.kafka.sink.KafkaSink;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This class implements a Kafka sink to publish Siddhi events to multiple kafka clusters. This sink is useful in
 * multi data center deployments where we have two identical setups replicated in two physical locations
 */
@Extension(
        name = "kafkaMultiDC",
        namespace = "sink",
        description = "A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka " +
                "cluster. The events can be published in the `TEXT` `XML` or `JSON` format.\n" +
                "If the topic is not already created in the Kafka cluster, the Kafka sink creates the default " +
                "partition for the given topic. The publishing topic and partition can be a dynamic value taken " +
                "from the Siddhi event.\n" +
                "To configure a sink to publish events via the Kafka transport, and using two Kafka brokers to " +
                "publish events to the same topic, the `type` parameter must have `kafkaMultiDC` as its value.",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                        description = " This parameter specifies the list of Kafka servers to which the Kafka " +
                                "sink must publish events. This list should be provided as a set of comma " +
                                "separated values. There must be " +
                                "at least two servers in this list. e.g., `localhost:9092,localhost:9093`.",
                        type = {DataType.STRING}),
                @Parameter(name = "topic",
                        description = "The topic to which the Kafka sink needs to publish events. Only one " +
                                "topic must be specified.",
                        type = {DataType.STRING}),
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
                @Parameter(name = "partition.no",
                        description = "The partition number for the given topic. Only one partition ID can be " +
                                "defined. If no value is specified for this parameter, the Kafka sink publishes " +
                                "to the default partition of the topic (i.e., 0)",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "0"),
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
                                "@sink("
                                + "type='kafkaMultiDC', "
                                + "topic='myTopic', "
                                + "partition.no='0',"
                                + "bootstrap.servers='host1:9092, host2:9092', "
                                + "@map(type='xml'))" +
                                "Define stream BarStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n",
                        description = "This query publishes to the  default (i.e., 0th) partition of the brokers in " +
                                "two data centers ")
        }
)
public class KafkaMultiDCSink extends KafkaSink {
    List<Producer<String, String>> producers = new ArrayList<>();
    private String topic;
    private Integer partitionNo;
    private static final Logger LOG = Logger.getLogger(KafkaMultiDCSink.class);

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                       ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        super.init(outputStreamDefinition, optionHolder, sinkConfigReader, siddhiAppContext);
        topic = optionHolder.validateAndGetStaticValue(KAFKA_PUBLISH_TOPIC);
        partitionNo = Integer.parseInt(optionHolder.validateAndGetStaticValue(KAFKA_PARTITION_NO, "0"));
        if (bootstrapServers.split(",").length != 2) {
            throw new SiddhiAppValidationException("There should be two servers listed in 'bootstrap.servers' " +
                    "configuration");
        }
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{KAFKA_MESSAGE_KEY};
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        readOptionalConfigs(props, optionalConfigs);

        String[] bootstrapServersList = bootstrapServers.split(",");
        for (int index = 0; index < bootstrapServersList.length; index++) {
            String server = bootstrapServersList[index].trim();
            props.put("bootstrap.servers", server);
            Producer<String, String> producer = new KafkaProducer<>(props);
            producers.add(producer);
            LOG.info("Kafka producer created for Kafka cluster :" + server);
        }
    }


    @Override
    public void publish(Object payload, DynamicOptions transportOptions) throws ConnectionUnavailableException {
        String key = keyOption.getValue(transportOptions);

        StringBuilder strPayload = new StringBuilder();
        strPayload.append(sequenceId).append(SEQ_NO_HEADER_FIELD_SEPERATOR).append(lastSentSequenceNo)
                .append(SEQ_NO_HEADER_DELIMITER)
                .append(payload.toString());
        lastSentSequenceNo.incrementAndGet();

        for (Producer producer : producers) {
            try {
                producer.send(new ProducerRecord<>(topic, partitionNo, key, strPayload.toString()));
            } catch (Exception e) {
                LOG.error(String.format("Failed to publish the message to [topic] %s. Error: %s. Sequence Number " +
                        ": %d", topic, e.getMessage(), lastSentSequenceNo.get() - 1), e);
            }
        }
    }

    @Override
    public void disconnect() {
        for (Producer producer : producers) {
            producer.flush();
            producer.close();
        }
        producers.clear();
    }
}
