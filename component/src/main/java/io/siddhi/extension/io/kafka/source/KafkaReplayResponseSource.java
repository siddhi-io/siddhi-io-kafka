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
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.stream.input.source.SourceSyncCallback;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.kafka.Constants;
import io.siddhi.extension.io.kafka.KafkaIOUtils;
import io.siddhi.extension.io.kafka.util.KafkaReplayResponseSourceRegistry;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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

public class KafkaReplayResponseSource extends Source<KafkaReplayResponseSource.KafkaReplaySourceState> implements SourceSyncCallback {
    public static final String SINGLE_THREADED = "single.thread";
    static final String TOPIC_WISE = "topic.wise";
    static final String PARTITION_WISE = "partition.wise";
    public static final String ADAPTOR_SUBSCRIBER_TOPIC = "topic.list";
    public static final String ADAPTOR_SUBSCRIBER_GROUP_ID = "group.id";
    public static final String ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS = "bootstrap.servers";
    public static final String ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST = "partition.no.list";
    public static final String ADAPTOR_ENABLE_AUTO_COMMIT = "enable.auto.commit";
    public static final String ADAPTOR_ENABLE_OFFSET_COMMIT = "enable.offsets.commit";
    public static final String ADAPTOR_ENABLE_ASYNC_COMMIT = "enable.async.commit";
    public static final String ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    private static final String TOPIC_OFFSET_MAP = "topic.offsets.map";
    public static final String THREADING_OPTION = "threading.option";
    public static final String SEQ_ENABLED = "seq.enabled";
    private static final String LAST_RECEIVED_SEQ_NO_KEY = "lastReceivedSeqNo";
    public static final String IS_BINARY_MESSAGE = "is.binary.message";
    private static final Logger LOG = Logger.getLogger(KafkaReplayResponseSource.class);
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offSet";
    private OptionHolder optionHolder;
    private ConsumerKafkaGroup consumerKafkaGroup;
    private String bootstrapServers;
    private String groupID;
    private String[] partitions;
    private String[] topics;
    private String optionalConfigs;
    private boolean seqEnabled = false;
    private boolean isBinaryMessage;
    private boolean enableOffsetCommit;
    private boolean enableAsyncCommit;
    private String topicOffsetMapConfig;
    private SiddhiAppContext siddhiAppContext;
    private KafkaReplayResponseSource.KafkaReplaySourceState KafkaReplaySourceState;
    private String threadingOption;
    private SourceEventListener sourceEventListener;
    private String[] requiredProperties;

    @Override
    public StateFactory<KafkaReplayResponseSource.KafkaReplaySourceState> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                                           String[] requiredProperties, ConfigReader configReader,
                                                           SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.optionHolder = optionHolder;
        this.requiredProperties = requiredProperties.clone();
        this.sourceEventListener = sourceEventListener;
        bootstrapServers = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS);
        groupID = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_GROUP_ID);
        threadingOption = optionHolder.validateAndGetStaticValue(THREADING_OPTION);
        String partitionList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_PARTITION_NO_LIST, null);
        partitions = (partitionList != null) ? partitionList.split(KafkaIOUtils.HEADER_SEPARATOR) : null;
        String topicList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_TOPIC);
        topics = topicList.split(KafkaIOUtils.HEADER_SEPARATOR);
        seqEnabled = optionHolder.validateAndGetStaticValue(SEQ_ENABLED, "false").equalsIgnoreCase("true");
        optionalConfigs = optionHolder.validateAndGetStaticValue(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES, null);
        isBinaryMessage = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(IS_BINARY_MESSAGE,
                "false"));
        enableOffsetCommit = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_OFFSET_COMMIT,
                "true"));
        enableAsyncCommit = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(ADAPTOR_ENABLE_ASYNC_COMMIT,
                "true"));
        topicOffsetMapConfig = optionHolder.validateAndGetStaticValue(TOPIC_OFFSET_MAP, null);
        if (PARTITION_WISE.equals(threadingOption) && null == partitions) {
            throw new SiddhiAppValidationException("Threading option is selected as 'partition.wise' but " +
                    "there are no partitions given");
        }

        return () -> new KafkaReplayResponseSource.KafkaReplaySourceState(seqEnabled);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, ByteBuffer.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, KafkaReplayResponseSource.KafkaReplaySourceState KafkaReplaySourceState)
            throws ConnectionUnavailableException {
        try {
            ExecutorService executorService = siddhiAppContext.getExecutorService();
            consumerKafkaGroup =
                    new ConsumerKafkaGroup(
                            topics, partitions,
                            KafkaReplayResponseSource.createConsumerConfig(bootstrapServers, groupID, optionalConfigs,
                                    isBinaryMessage, enableOffsetCommit),
                            threadingOption, executorService, isBinaryMessage, enableOffsetCommit, enableAsyncCommit,
                            sourceEventListener, requiredProperties);
            checkTopicsAvailableInCluster();
            checkPartitionsAvailableForTheTopicsInCluster();
            this.KafkaReplaySourceState = KafkaReplaySourceState;
            // If state does not contain the topic offset map try to read it from the config
            if (!KafkaReplaySourceState.isRestored && topicOffsetMapConfig != null) {
                synchronized (KafkaReplaySourceState) {
                    KafkaReplaySourceState.topicOffsetMap = readTopicOffsetsConfig(topicOffsetMapConfig);
                }
                consumerKafkaGroup.setKafkaSourceState(KafkaReplaySourceState);
                consumerKafkaGroup.restoreState();
            } else {
                consumerKafkaGroup.setKafkaSourceState(KafkaReplaySourceState);
            }
            consumerKafkaGroup.run();
        } catch (SiddhiAppRuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConnectionUnavailableException("Error when initiating connection with Kafka server: " +
                    bootstrapServers + " in Siddhi App: " + siddhiAppContext.getName(), e);
        }
    }

    @Override
    public void disconnect() {
        this.KafkaReplaySourceState = null;
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
                    KafkaReplaySourceState.topicOffsetMap.computeIfAbsent(keyValues[1], k -> new HashMap<>());
                } else if (keyValues[0].equals(PARTITION)) {
                    Map<Integer, Long> partitionOffsetMap = KafkaReplaySourceState.topicOffsetMap.get(topic);
                    if (null == partitionOffsetMap.get(Integer.valueOf(keyValues[1]))) {
                        partition = Integer.valueOf(keyValues[1]);
                        partitionOffsetMap.put(partition, 0L);
                    }
                } else if (keyValues[0].equals(OFFSET)) {
                    Map<Integer, Long> partitionOffsetMap = KafkaReplaySourceState.topicOffsetMap.get(topic);
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

    private void checkTopicsAvailableInCluster() {
        Properties props = KafkaReplayResponseSource.createConsumerConfig(bootstrapServers, groupID, optionalConfigs,
                isBinaryMessage, enableOffsetCommit);
        props.put("group.id", "test-consumer-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
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
            throw new SiddhiAppRuntimeException("Topic(s) " + invalidTopics + " aren't available. "
                    + "Topics won't be created since there "
                    + "are partition numbers defined in the query.");
        } else if (!topicsAvailable) {
            if (siddhiAppContext.isTransportChannelCreationEnabled()) {
                LOG.warn("Topic(s) " + invalidTopics + " aren't available. "
                        + "These Topics will be created with the default partition.");
            } else {
                throw new SiddhiAppRuntimeException("Topic(s) " + invalidTopics + " creation failed. " +
                        "User has disabled topic creation by setting " +
                        SiddhiConstants.TRANSPORT_CHANNEL_CREATION_IDENTIFIER +
                        " property to false. Hence Siddhi App deployment will be aborted.");
            }
        }
    }

    private void checkPartitionsAvailableForTheTopicsInCluster() {
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
                    throw new SiddhiAppRuntimeException(
                            "Partition number(s) " + invalidPartitions + " aren't available for "
                                    + "the topic: " + topic);
                }
            }
        }
    }

    protected static Properties createConsumerConfig(String zkServerList, String groupId, String optionalConfigs,
                                                     boolean isBinaryMessage, boolean enableOffsetCommit) {
        Properties props = new Properties();
        props.put(ADAPTOR_SUBSCRIBER_ZOOKEEPER_CONNECT_SERVERS, zkServerList);
        props.put(ADAPTOR_SUBSCRIBER_GROUP_ID, groupId);

        //If it stops heart-beating for a period of time longer than session.timeout.ms then it will be considered dead
        // and its partitions will be assigned to another process
        props.put("session.timeout.ms", "30000");
        if (!enableOffsetCommit) {
            props.put(ADAPTOR_ENABLE_AUTO_COMMIT, "false");
        }
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        if (!isBinaryMessage) {
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        } else {
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }

        KafkaIOUtils.splitHeaderValues(optionalConfigs, props);
        return props;
    }

    private static Properties createProducerConfig(String zkServerList, String optionalConfigs,
                                                   boolean isBinaryMessage) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, zkServerList);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaIOUtils.splitHeaderValues(optionalConfigs, configProperties);

        if (!isBinaryMessage) {
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
        } else {
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
        }
        return configProperties;
    }


    /**
     * State class for Kafka source.
     */
    public class KafkaReplaySourceState extends State {

        private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
        private Map<String, Map<SequenceKey, Integer>> consumerLastReceivedSeqNoMap = null;
        private boolean isRestored = false;

        public Map<String, Map<Integer, Long>> getTopicOffsetMap() {
            return topicOffsetMap;
        }

        public Map<String, Map<SequenceKey, Integer>> getConsumerLastReceivedSeqNoMap() {
            return consumerLastReceivedSeqNoMap;
        }

        public KafkaReplaySourceState(boolean seqEnabled) {
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
            if (consumerKafkaGroup != null) {
                consumerKafkaGroup.restoreState();
            }
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
