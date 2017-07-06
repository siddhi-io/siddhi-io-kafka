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

package org.wso2.extension.siddhi.io.kafka.source;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;
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
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class implements a Kafka source to receives events from a kafka cluster.
 */
@Extension(
        name = "kafka",
        namespace = "source",
        description = "The Kafka Sink publishes records to a topic with a partition for a Kafka cluster which are in "
                + "format such as `text`, `XML` and `JSON`.\n"
                + "The Kafka Sink will create the default partition for a given topic, if the topic is not already "
                + "been created in the Kafka cluster.",
        parameters = {
                @Parameter(name = "topic.list",
                           description = "The topic list which the source would be listening to. This should be given "
                                   + "in comma separated values. eg: 'topic_one,topic_two' ",
                           type = {DataType.STRING}),
                @Parameter(name = "partition.no.list",
                           description = "The partition number list for the given topic. This should be given in "
                                   + "comma separated values. eg: '0,1,1' ",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "0"),
                @Parameter(name = "group.id",
                           description = "This is used to identify the Kafka source group. And sources with same "
                                   + "topic and partition which are in the same group wont receive the same event",
                           type = {DataType.STRING}),
                @Parameter(name = "threading.option",
                           description = "Each source can be run in either single thread or in multi threads. The "
                                   + "threading options are `single.thread`, `topic.wise` and `partition.wise` ",
                           type = {DataType.STRING}),
                @Parameter(name = "bootstrap.servers",
                           description = "This should contain the kafka server list which the kafka source should be "
                                   + "listening to. This should be given in comma separated values. "
                                   + "eg: 'localhost:9092,localhost:9093' ",
                           type = {DataType.STRING})
        },
        examples = {
                @Example(
                        description = "The following query will listen to 'kafka_topic' and 'kafka_topic2' topics "
                                + "with 0 and 1 partitions. There will be a thread created for each topic and "
                                + "partition combination. The receiving xml events will be mapped to a siddhi event "
                                + "and will be send to the FooStream.",
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source("
                                    + "type='kafka', "
                                    + "topic.list='kafka_topic,kafka_topic2', "
                                    + "group.id='test', "
                                    + "threading.option='partition.wise', "
                                    + "bootstrap.servers='localhost:9092', "
                                    + "partition.no.list='0,1', "
                                    + "@map(type='xml'))\n" +
                                "Define stream FooStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n"),
                @Example(
                        description = "The following query will listen to 'kafka_topic' topic for the default "
                                + "partition since there is no 'partition.no.list' is defined. There will be a only "
                                + "one thread created for the topic. The receiving xml events will be mapped to a "
                                + "siddhi event and will be send to the FooStream.",
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source("
                                    + "type='kafka', "
                                    + "topic.list='kafka_topic', "
                                    + "group.id='test', "
                                    + "threading.option='single.thread', "
                                    + "bootstrap.servers='localhost:9092', "
                                    + "@map(type='xml'))\n" +
                                "Define stream FooStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n")
        }
)
public class KafkaSource extends Source {

    protected static final String SINGLE_THREADED = "single.thread";
    protected static final String TOPIC_WISE = "topic.wise";
    protected static final String PARTITION_WISE = "partition.wise";
    private static final Logger LOG = Logger.getLogger(KafkaSource.class);
    private static final String ADAPTOR_SUBSCRIBER_TOPIC = "topic.list";
    private static final String ADAPTOR_SUBSCRIBER_GROUP_ID = "group.id";
    private static final String ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS = "bootstrap.servers";
    private static final String ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST = "partition.no.list";
    private static final String ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    private static final String TOPIC_OFFSET_MAP = "topic.offset.map";
    private static final String THREADING_OPTION = "threading.option";
    private static final String HEADER_SEPARATOR = ",";
    private static final String ENTRY_SEPARATOR = ":";
    private SourceEventListener sourceEventListener;
    private ScheduledExecutorService executorService;
    private OptionHolder optionHolder;
    private ConsumerKafkaGroup consumerKafkaGroup;
    private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
    private String bootstrapServers;
    private String groupID;
    private String threadingOption;
    private String partitions[];
    private String topics[];
    private String optionalConfigs;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] strings,
                               ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.optionHolder = optionHolder;
        this.executorService = siddhiAppContext.getScheduledExecutorService();
        bootstrapServers = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS);
        groupID = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_GROUP_ID);
        threadingOption = optionHolder.validateAndGetStaticValue(THREADING_OPTION);
        String partitionList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST, null);
        partitions = (partitionList != null) ? partitionList.split(HEADER_SEPARATOR) : null;
        String topicList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_TOPIC);
        topics = topicList.split(HEADER_SEPARATOR);
        optionalConfigs = optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        checkTopicsAvailableInCluster();
        checkPartitionsAvailableForTheTopicsInCluster();
        if (KafkaSource.PARTITION_WISE.equals(threadingOption) && null == partitions) {
            throw new SiddhiAppValidationException("The threading option is 'partition.wise' but there are no "
                                                           + "partition numbers defined.");
        }
        siddhiAppContext.getSnapshotService().addSnapshotable("kafka-sink", this);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        consumerKafkaGroup = new ConsumerKafkaGroup(topics, partitions,
                                                    KafkaSource.createConsumerConfig(bootstrapServers, groupID,
                                                                                     optionalConfigs),
                                                    topicOffsetMap, threadingOption, this.executorService);
        consumerKafkaGroup.run(sourceEventListener);
    }

    @Override
    public void disconnect() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.shutdown();
            LOG.debug("Kafka Adapter disconnected for topic/s" +
                              optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_TOPIC));
        }
    }

    @Override
    public void destroy() {
        consumerKafkaGroup = null;
    }

    @Override
    public void pause() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.pause();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka Adapter paused for topic/s" + optionHolder.validateAndGetStaticValue
                        (ADAPTOR_SUBSCRIBER_TOPIC));
            }
        }
    }

    @Override
    public void resume() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.resume();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka Adapter resumed for topic/s" + optionHolder.validateAndGetStaticValue
                        (ADAPTOR_SUBSCRIBER_TOPIC));
            }
        }
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put(TOPIC_OFFSET_MAP, this.topicOffsetMap);
        return currentState;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        this.topicOffsetMap = (Map<String, Map<Integer, Long>>) state.get(TOPIC_OFFSET_MAP);
        consumerKafkaGroup.restore(topicOffsetMap);
    }

    private void checkTopicsAvailableInCluster() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        Map<String, List<PartitionInfo>> testTopicList = consumer.listTopics();
        boolean topicsAvailable = true;
        StringBuilder invalidTopics = new StringBuilder("");
        for (String topic : topics) {
            boolean topicAvailable = false;
            for (Map.Entry<String, List<PartitionInfo>> entry : testTopicList.entrySet()) {
                if (entry.getKey().equals(topic)) {
                    topicAvailable = true;
                }
            }
            if (!topicAvailable) {
                topicsAvailable = false;
                if ("".equals(invalidTopics.toString())) {
                    invalidTopics.append(topic);
                } else {
                    invalidTopics.append(',').append(topic);
                }
                LOG.warn("Topic, " + topic + " is not available.");
            }
        }
        if (null != partitions && !topicsAvailable) {
            String errorMessage = "Topic/s " + invalidTopics + " aren't available. Topics wont created since there "
                    + "are partition numbers defined in the query.";
            LOG.error(errorMessage);
            throw new SiddhiAppValidationException(errorMessage);
        } else if (!topicsAvailable) {
            LOG.warn("Topic/s " + invalidTopics + " aren't available. These Topics will be created with the default "
                             + "partition.");
        }
    }

    private void checkPartitionsAvailableForTheTopicsInCluster() {
        //checking whether the defined partitions are available in the defined topic
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                             "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                             "org.apache.kafka.common.serialization.StringSerializer");
        if (PARTITION_WISE.equals(threadingOption) && null == partitions) {
            throw new SiddhiAppValidationException("Threading option is selected as 'partition.wise' but there are no"
                                                           + " partitions given");
        }
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        boolean partitionsAvailable = true;
        StringBuilder invalidPartitions = new StringBuilder("");
        for (String topic : topics) {
            List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
            if (null != partitions) {
                for (String partition : partitions) {
                    boolean partitonAvailable = false;
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        if (Integer.parseInt(partition) == partitionInfo.partition()) {
                            partitonAvailable = true;
                        }
                    }
                    if (!partitonAvailable) {
                        partitionsAvailable = false;
                        if ("".equals(invalidPartitions.toString())) {
                            invalidPartitions.append(partition);
                        } else {
                            invalidPartitions.append(',').append(partition);
                        }
                        LOG.error("Partition number, " + partition
                                          + " in 'partition.id' is not available in topic partitions");
                    }
                }
                if (!partitionsAvailable) {
                    throw new SiddhiAppValidationException(
                            "Partition number/s " + invalidPartitions + " aren't available for "
                                    + "the topic: " + topic);
                }
            }
        }
    }

    private static Properties createConsumerConfig(String zkServerList, String groupId, String optionalConfigs) {
        Properties props = new Properties();
        props.put(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS, zkServerList);
        props.put(ADAPTOR_SUBSCRIBER_GROUP_ID, groupId);

        //If it stops heart-beating for a period of time longer than session.timeout.ms then it will be considered dead
        // and its partitions will be assigned to another process
        props.put("session.timeout.ms", "30000");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (optionalConfigs != null) {
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
        return props;
    }
}
