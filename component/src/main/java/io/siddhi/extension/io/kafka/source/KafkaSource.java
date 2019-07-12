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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.stream.input.source.SourceSyncCallback;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;


/**
 * This class implements a Kafka source to receive events from a kafka cluster.
 */
@Extension(
        name = "kafka",
        namespace = "source",
        description = "A Kafka source receives events to be processed by WSO2 SP from a topic with a partition " +
                "for a Kafka cluster. The events received can be in the `TEXT` `XML` `JSON` or `Binary` format.\n" +
                "If the topic is not already created in the Kafka cluster, the Kafka sink creates the default " +
                "partition for the given topic.",
        parameters = {
                @Parameter(name = "bootstrap.servers",
                        description = "This specifies the list of Kafka servers to which the Kafka source " +
                                "must listen. This list can be provided as a set of comma-separated values.\n" +
                                "e.g., `localhost:9092,localhost:9093`",
                        type = {DataType.STRING}),
                @Parameter(name = "topic.list",
                        description = "This specifies the list of topics to which the source must listen. This " +
                                "list can be provided as a set of comma-separated values.\n" +
                                "e.g., `topic_one,topic_two`",
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
                @Parameter(name = "partition.no.list",
                        description = "The partition number list for the given topic. This is provided as a list" +
                                " of comma-separated values. e.g., `0,1,2,`.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "0"),
                @Parameter(name = "seq.enabled",
                        description = "If this parameter is set to `true`, the sequence of the events received via" +
                                " the source is taken into account. Therefore, each event should contain a " +
                                "sequence number as an attribute value to indicate the sequence.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "is.binary.message",
                        description = "In order to receive binary events via the Kafka source,it is required to set" +
                                "this parameter to 'True'.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "topic.offsets.map",
                        description = "This parameter specifies reading offsets for each topic and partition. " +
                                "The value for this parameter is specified in the following format: \n " +
                                "`<topic>=<offset>,<topic>=<offset>,`\n " +
                                " When an offset is defined for a topic, the Kafka source skips reading the " +
                                "message with the number specified as the offset as well as all the messages sent" +
                                " previous to that message. If the offset is not defined for a specific topic it " +
                                "reads messages from the beginning. \n" +
                                "e.g., `stocks=100,trades=50` reads from the 101th message of the `stocks` topic, and" +
                                " from the 51st message of the `trades` topic.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "optional.configuration",
                        description = "This parameter contains all the other possible configurations that the " +
                                "consumer is created with. \n" +
                                "e.g., `ssl.keystore.type:JKS,batch.size:200`.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null")
        },
        examples = {
                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source(\n" +
                                "type='kafka', \n" +
                                "topic.list='kafka_topic,kafka_topic2', \n" +
                                "group.id='test', \n" +
                                "threading.option='partition.wise', \n" +
                                "bootstrap.servers='localhost:9092', \n" +
                                "partition.no.list='0,1', \n" +
                                "@map(type='xml'))\n" +
                                "Define stream FooStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n",
                        description = "This kafka source configuration listens to the `kafka_topic` and " +
                                "`kafka_topic2` topics with `0` and `1` partitions. A thread is created for each " +
                                "topic and partition combination. The events are received in the XML format, mapped" +
                                " to a Siddhi event, and sent to a stream named `FooStream`. "),
                @Example(
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream BarStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source(\n" +
                                "type='kafka', \n" +
                                "topic.list='kafka_topic',\n" +
                                "group.id='test', \n" +
                                "threading.option='single.thread',\n" +
                                "bootstrap.servers='localhost:9092',\n" +
                                "@map(type='xml'))\n" +
                                "Define stream FooStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n",
                        description = "This Kafka source configuration listens to the `kafka_topic` topic for the " +
                                "default partition because no `partition.no.list` is defined. Only one thread is " +
                                "created for the topic. The events are received in the XML format, mapped" +
                                " to a Siddhi event, and sent to a stream named `FooStream`.")
        }
)
public class KafkaSource extends Source<KafkaSource.KafkaSourceState> implements SourceSyncCallback {

    public static final String SINGLE_THREADED = "single.thread";
    public static final String TOPIC_WISE = "topic.wise";
    public static final String PARTITION_WISE = "partition.wise";
    public static final String ADAPTOR_SUBSCRIBER_TOPIC = "topic.list";
    public static final String ADAPTOR_SUBSCRIBER_GROUP_ID = "group.id";
    public static final String ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS = "bootstrap.servers";
    public static final String ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST = "partition.no.list";
    public static final String ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    public static final String TOPIC_OFFSET_MAP = "topic.offsets.map";
    public static final String THREADING_OPTION = "threading.option";
    public static final String SEQ_ENABLED = "seq.enabled";
    public static final String HEADER_SEPARATOR = ",";
    public static final String ENTRY_SEPARATOR = ":";
    public static final String LAST_RECEIVED_SEQ_NO_KEY = "lastReceivedSeqNo";
    public static final String IS_BINARY_MESSAGE = "is.binary.message";
    private static final Logger LOG = Logger.getLogger(KafkaSource.class);
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offSet";
    private SourceEventListener sourceEventListener;
    private ScheduledExecutorService executorService;
    private OptionHolder optionHolder;
    private ConsumerKafkaGroup consumerKafkaGroup;
    private String bootstrapServers;
    private String groupID;
    private String threadingOption;
    private String partitions[];
    private String topics[];
    private String optionalConfigs;
    private boolean seqEnabled = false;
    private boolean isBinaryMessage;
    private String topicOffsetMapConfig;
    private SiddhiAppContext siddhiAppContext;
    private KafkaSourceState kafkaSourceState;

    private static Properties createConsumerConfig(String zkServerList, String groupId, String optionalConfigs,
                                                   boolean isBinaryMessage) {
        Properties props = new Properties();
        props.put(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS, zkServerList);
        props.put(ADAPTOR_SUBSCRIBER_GROUP_ID, groupId);

        //If it stops heart-beating for a period of time longer than session.timeout.ms then it will be considered dead
        // and its partitions will be assigned to another process
        props.put("session.timeout.ms", "30000");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (!isBinaryMessage) {
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        } else {
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }

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
        return props;
    }

    private static Properties createProducerConfig(String zkServerList, String optionalConfigs,
                                                   boolean isBinaryMessage) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, zkServerList);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (optionalConfigs != null && !optionalConfigs.isEmpty()) {
            String[] optionalProperties = optionalConfigs.split(HEADER_SEPARATOR);
            if (optionalProperties.length > 0) {
                for (String header : optionalProperties) {
                    try {
                        String[] configPropertyWithValue = header.split(ENTRY_SEPARATOR, 2);
                        configProperties.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                    } catch (Exception e) {
                        LOG.warn("Optional property '" + header + "' is not defined in the correct format.", e);
                    }
                }
            }
        }
        if (!isBinaryMessage) {
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
        } else {
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
        }
        return configProperties;
    }

    @Override
    public StateFactory<KafkaSourceState> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                               String[] strings, ConfigReader configReader,
                                               SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
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
        seqEnabled = optionHolder.validateAndGetStaticValue(SEQ_ENABLED, "false").equalsIgnoreCase("true");
        optionalConfigs = optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        isBinaryMessage = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(IS_BINARY_MESSAGE,
                "false"));
        topicOffsetMapConfig = optionHolder.validateAndGetStaticValue(TOPIC_OFFSET_MAP, null);
        if (PARTITION_WISE.equals(threadingOption) && null == partitions) {
            throw new SiddhiAppValidationException("Threading option is selected as 'partition.wise' but there are no"
                    + " partitions given");
        }
        checkTopicsAvailableInCluster();
        checkPartitionsAvailableForTheTopicsInCluster();

        consumerKafkaGroup = new ConsumerKafkaGroup(topics, partitions,
                KafkaSource.createConsumerConfig(bootstrapServers, groupID, optionalConfigs, isBinaryMessage),
                threadingOption, executorService, isBinaryMessage, sourceEventListener);

        return () -> new KafkaSourceState(seqEnabled);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, ByteBuffer.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, KafkaSourceState kafkaSourceState)
            throws ConnectionUnavailableException {
        this.kafkaSourceState = kafkaSourceState;
        // If state does not contain the topic offset map try to read it from the config
        if (!kafkaSourceState.isRestored && topicOffsetMapConfig != null) {
            synchronized (kafkaSourceState) {
                kafkaSourceState.topicOffsetMap = readTopicOffsetsConfig(topicOffsetMapConfig);
            }
            consumerKafkaGroup.setKafkaSourceState(kafkaSourceState);
            consumerKafkaGroup.restoreState();
        } else {
            consumerKafkaGroup.setKafkaSourceState(kafkaSourceState);
        }
        consumerKafkaGroup.run();
    }

    @Override
    public void disconnect() {
        this.kafkaSourceState = null;
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.setKafkaSourceState(null);
            consumerKafkaGroup.shutdown();
            LOG.info("Kafka Adapter disconnected for topic(s): " +
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
            LOG.info("Kafka Adapter paused for topic(s): " + optionHolder.validateAndGetStaticValue
                    (ADAPTOR_SUBSCRIBER_TOPIC));
        }
    }

    @Override
    public void resume() {
        if (consumerKafkaGroup != null) {
            consumerKafkaGroup.resume();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka Adapter resumed for topic(s): " + optionHolder.validateAndGetStaticValue
                        (ADAPTOR_SUBSCRIBER_TOPIC));
            }
        }
    }

    private Map<String, Map<Integer, Long>> readTopicOffsetsConfig(String topicOffsetsConfig) {
        Map<String, Map<Integer, Long>> perTopicPerPartitionOffset = new HashMap<>();
        String[] topicOffsets = topicOffsetsConfig.split(",");
        for (String entry : topicOffsets) {
            String[] topicOffset = entry.split("=");
            if (topicOffset.length != 2) {
                LOG.error("Topic offset should be given in <topic>=<offset>,.. format. ");
                return null;
            }

            boolean isTopicListed = Arrays.stream(topics).anyMatch(topic -> topic.equals(topicOffset[0]));
            if (!isTopicListed) {
                LOG.error("Topic " + topicOffset[0] + " not listed in topic.list config");
                return null;
            }

            Map<Integer, Long> partitionOffset = new HashMap<>();
            Arrays.stream(partitions).forEach(partition -> {
                partitionOffset.put(Integer.parseInt(partition), Long.parseLong(topicOffset[1]));
            });

            perTopicPerPartitionOffset.put(topicOffset[0], partitionOffset);
        }
        return perTopicPerPartitionOffset;
    }

    private void checkTopicsAvailableInCluster() throws SiddhiAppValidationException {
        Properties props = KafkaSource.createConsumerConfig(bootstrapServers, groupID, optionalConfigs,
                isBinaryMessage);
        props.put("group.id", "test-consumer-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
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
            if (null != partitions && !(partitions.length == 1 && partitions[0].equals("0")) && !topicsAvailable) {
                String errorMessage = "Topic(s) " + invalidTopics + " aren't available. Topics won't be created "
                        + "since there are partition numbers defined in the query.";
                LOG.error(errorMessage);
                throw new SiddhiAppValidationException("Topic(s) " + invalidTopics + " aren't available. "
                        + "Topics won't be created since there "
                        + "are partition numbers defined in the query.");
            } else if (!topicsAvailable) {
                if (siddhiAppContext.isTransportChannelCreationEnabled()) {
                    LOG.warn("Topic(s) " + invalidTopics + " aren't available. "
                            + "These Topics will be created with the default partition.");
                } else {
                    throw new SiddhiAppCreationException("Topic(s) " + invalidTopics + " creation failed. " +
                            "User has disabled topic creation by setting " +
                            SiddhiConstants.TRANSPORT_CHANNEL_CREATION_IDENTIFIER +
                            " property to false. Hence Siddhi App deployment will be aborted.");
                }
            }
        } catch (NullPointerException ex) {
            //calling super class logs the exception and retry
            throw new SiddhiAppValidationException("Exception when connecting to kafka servers: "
                    + sourceEventListener.getStreamDefinition().getId(), ex);
        }
    }

    private void checkPartitionsAvailableForTheTopicsInCluster() throws SiddhiAppValidationException {
        //checking whether the defined partitions are available in the defined topic
        Properties configProperties = createProducerConfig(bootstrapServers, optionalConfigs, isBinaryMessage);
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        boolean partitionsAvailable = true;
        StringBuilder invalidPartitions = new StringBuilder("");
        for (String topic : topics) {
            List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
            if (null != partitions && !(partitions.length == 1 && partitions[0].equals("0"))) {
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
                            "Partition number(s) " + invalidPartitions + " aren't available for "
                                    + "the topic: " + topic);
                }
            }
        }
    }

    @Override
    public void update(String[] transportSyncProperties) {
        //here we are handling out of order events which could occur
        for (String propertiesStr : transportSyncProperties) {
            String[] properties = propertiesStr.split(",");
            String topic = "";
            Integer partition = 0;
            for (String property : properties) {
                String[] keyValues = property.split(":");
                if (keyValues[0].equals(TOPIC)) {
                    topic = keyValues[1];
                    kafkaSourceState.topicOffsetMap.computeIfAbsent(keyValues[1], k -> new HashMap<>());
                } else if (keyValues[0].equals(PARTITION)) {
                    Map<Integer, Long> partitionOffsetMap = kafkaSourceState.topicOffsetMap.get(topic);
                    if (null == partitionOffsetMap.get(Integer.valueOf(keyValues[1]))) {
                        partition = Integer.valueOf(keyValues[1]);
                        partitionOffsetMap.put(partition, 0L);
                    }
                } else if (keyValues[0].equals(OFFSET)) {
                    Map<Integer, Long> partitionOffsetMap = kafkaSourceState.topicOffsetMap.get(topic);
                    long savedOffsetValue = partitionOffsetMap.get(partition);
                    Long offsetValue = Long.valueOf(keyValues[1]);
                    if (offsetValue > savedOffsetValue) {
                        partitionOffsetMap.put(partition, offsetValue);
                    }
                }
            }
        }
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * State class for Kafka source.
     */
    public class KafkaSourceState extends State {

        private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
        private Map<String, Map<SequenceKey, Integer>> consumerLastReceivedSeqNoMap = null;
        private boolean isRestored = false;

        public Map<String, Map<Integer, Long>> getTopicOffsetMap() {
            return topicOffsetMap;
        }

        public Map<String, Map<SequenceKey, Integer>> getConsumerLastReceivedSeqNoMap() {
            return consumerLastReceivedSeqNoMap;
        }

        public KafkaSourceState(boolean seqEnabled) {
            if (seqEnabled) {
                consumerLastReceivedSeqNoMap = new HashMap<>();
            }
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> currentState = new HashMap<>();
            currentState.put(TOPIC_OFFSET_MAP, this.topicOffsetMap);
            if (seqEnabled) {
                currentState.put(LAST_RECEIVED_SEQ_NO_KEY, consumerLastReceivedSeqNoMap);
            }
            return currentState;
        }

        public void restore(Map<String, Object> state) {
            isRestored = true;
            topicOffsetMap = (Map<String, Map<Integer, Long>>) state.get(TOPIC_OFFSET_MAP);
            consumerKafkaGroup.restoreState();
            if (seqEnabled) {
                this.consumerLastReceivedSeqNoMap =
                        (Map<String, Map<SequenceKey, Integer>>) state.get(LAST_RECEIVED_SEQ_NO_KEY);
            }
        }

        @Override
        public boolean canDestroy() {
            return false;
        }
    }
}
