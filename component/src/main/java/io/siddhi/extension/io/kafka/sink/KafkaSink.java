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

package io.siddhi.extension.io.kafka.sink;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.kafka.Constants;
import io.siddhi.extension.io.kafka.KafkaIOUtils;
import io.siddhi.extension.io.kafka.metrics.SinkMetrics;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements a Kafka sink to publish Siddhi events to a kafka cluster.
 */
@Extension(
        name = "kafka",
        namespace = "sink",
        description = "A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka " +
                "cluster. The events can be published in the `TEXT` `XML` `JSON` or `Binary` format.\n" +
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
                @Parameter(name = "is.binary.message",
                        description = "In order to send the binary events via kafka sink, this " +
                                "parameter is set to 'True'.",
                        type = {DataType.BOOL},
                        optional = false,
                        defaultValue = "null"),
                @Parameter(name = "optional.configuration",
                        description = "This parameter contains all the other possible configurations that the " +
                                "producer is created with. \n" +
                                "e.g., `producer.type:async,batch.size:200`",
                        optional = true,
                        type = {DataType.STRING},
                        defaultValue = "null"),
                @Parameter(name = "is.synchronous",
                        description = "The Kafka sink will publish the events to the server synchronously when the" +
                                "value is set to `true`, and asynchronously if otherwise",
                        optional = true,
                        type = {DataType.BOOL},
                        defaultValue = "false")
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
                                "Siddhi event. The value for `partition.no` is taken from the `volume` attribute, " +
                                "and the topic value is taken from the `symbol` attribute.")
        }
)
public class KafkaSink extends Sink<KafkaSink.KafkaSinkState> {

    public static final String SEQ_NO_HEADER_DELIMITER = "~";
    public static final String SEQ_NO_HEADER_FIELD_SEPERATOR = ":";
    protected static final String KAFKA_PUBLISH_TOPIC = "topic";
    protected static final String IS_SYNCHRONOUS = "is.synchronous";
    protected static final String KAFKA_MESSAGE_KEY = "key";
    protected static final String KAFKA_PARTITION_NO = "partition.no";
    private static final String LAST_SENT_SEQ_NO_PERSIST_KEY = "lastSentSequenceNo";
    private static final String KAFKA_BROKER_LIST = "bootstrap.servers";
    private static final String KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    private static final String SEQ_ID = "sequence.id";
    private static final String IS_BINARY_MESSAGE = "is.binary.message";
    private static final Logger LOG = Logger.getLogger(KafkaSink.class);
    protected String bootstrapServers;
    protected String optionalConfigs;
    protected String sequenceId = null;
    protected Boolean isBinaryMessage;
    protected Option keyOption;
    private Option topicOption = null;
    private Option partitionOption;
    private Boolean isSequenced = false;
    private Producer<String, Object> producer;
    private SinkMetrics metrics;
    private String streamId;
    private boolean useAvroSerializer = false;
    private String schemaRegistry;
    private String siddhiAppName;
    private boolean isSynchronous;

    @Override
    protected StateFactory<KafkaSinkState> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                                ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {

        if (sinkConfigReader != null) {
            bootstrapServers = sinkConfigReader.readConfig(KAFKA_BROKER_LIST,
                    optionHolder.validateAndGetStaticValue(KAFKA_BROKER_LIST));
            optionalConfigs = sinkConfigReader.readConfig(KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES,
                    optionHolder.validateAndGetStaticValue(KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES, null));
            sequenceId = sinkConfigReader.readConfig(SEQ_ID, optionHolder.validateAndGetStaticValue(SEQ_ID, null));

        } else {
            bootstrapServers = optionHolder.validateAndGetStaticValue(KAFKA_BROKER_LIST);
            optionalConfigs = optionHolder.validateAndGetStaticValue(KAFKA_OPTIONAL_CONFIGURATION_PROPERTIES, null);
            sequenceId = optionHolder.validateAndGetStaticValue(SEQ_ID, null);
        }
        isSynchronous = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(IS_SYNCHRONOUS, "false"));
        topicOption = optionHolder.validateAndGetOption(KAFKA_PUBLISH_TOPIC);
        partitionOption = optionHolder.getOrCreateOption(KAFKA_PARTITION_NO, null);
        isBinaryMessage = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(IS_BINARY_MESSAGE,
                "false"));
        isSequenced = sequenceId != null;
        keyOption = optionHolder.getOrCreateOption(KAFKA_MESSAGE_KEY, null);
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                        Constants.PROMETHEUS_REPORTER_NAME)) {
                    metrics = new SinkMetrics(siddhiAppContext.getName(), streamId);
                    streamId = outputStreamDefinition.getId();
                }
            } catch (IllegalArgumentException e) {
                LOG.debug("Prometheus reporter is not running. Hence kafka metrics will not be initialized for "
                        + siddhiAppContext.getName());
            }
        }
        useAvroSerializer = useAvroSerializer(outputStreamDefinition);
        siddhiAppName = siddhiAppContext.getName();
        return () -> new KafkaSinkState(isSequenced);
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, KafkaSinkState kafkaSinkState)
            throws ConnectionUnavailableException {
        String topic = topicOption.getValue(dynamicOptions);
        String partitionNo = partitionOption.getValue(dynamicOptions);
        String key = keyOption.getValue(dynamicOptions);
        Object payloadToSend = null;

        try {
            //If the received payload is String.
            if (payload instanceof String) {

                // If it is required to send the message as string message with sequence numbers.
                if (isSequenced && !isBinaryMessage) {
                    StringBuilder strPayload = new StringBuilder();
                    strPayload.append(sequenceId).append(SEQ_NO_HEADER_FIELD_SEPERATOR).
                            append(kafkaSinkState.lastSentSequenceNo.get())
                            .append(SEQ_NO_HEADER_DELIMITER).append(payload.toString());
                    payloadToSend = strPayload.toString();
                    kafkaSinkState.lastSentSequenceNo.incrementAndGet();

                    // If it is required to send the message as string message without sequence numbers.
                } else if (!isSequenced && !isBinaryMessage) {
                    payloadToSend = payload.toString();

                    // If it is required to send the message as binary message with sequence numbers.
                } else if (isSequenced && isBinaryMessage) {
                    byte[] byteEvents = payload.toString().getBytes("UTF-8");
                    payloadToSend = getSequencedBinaryPayloadToSend(byteEvents, kafkaSinkState);
                    kafkaSinkState.lastSentSequenceNo.incrementAndGet();

                    // If it is required to send the message as binary message without sequence numbers.
                } else {
                    payloadToSend = payload.toString().getBytes("UTF-8");
                }

            } else if (useAvroSerializer) {  //Case: useAvroSerializer is true
                if (payload instanceof GenericRecord) {
                    payloadToSend = payload;
                } else {
                    LOG.error("use.avro.serializer property is set to true in Avro Mapper. Expected " +
                            "GenericRecord but received " + payload.getClass().getName() + " Hence dropping event!");
                }
            } else { //if the received payload to send is binary.
                byte[] byteEvents = ((ByteBuffer) payload).array();
                if (isSequenced) {
                    payloadToSend = getSequencedBinaryPayloadToSend(byteEvents, kafkaSinkState);
                    kafkaSinkState.lastSentSequenceNo.incrementAndGet();
                } else {
                    payloadToSend = byteEvents;
                }
            }
            if (isSynchronous) {
                long pushedTimestamp = System.currentTimeMillis();
                RecordMetadata metadata;
                try {
                    if (null == partitionNo) {
                        metadata = producer.send(new ProducerRecord<>(topic, null, System.currentTimeMillis(), key,
                                payloadToSend)).get();
                    } else {
                        Integer partition = Integer.parseInt(partitionNo);
                        long currentTime = System.currentTimeMillis();
                        metadata = producer.send(new ProducerRecord<>(topic, partition, currentTime,
                                key, payloadToSend)).get();
                    }
                    sendToMetrics(metadata, pushedTimestamp);
                } catch (ExecutionException e) {
                    metrics.getErrorCountWithoutPartition(
                            topic, streamId, e.getClass().getSimpleName()).inc();
                    // When the siddhi app user has not given a partition, then this stat won't be published.
                    if (partitionNo != null) {
                        metrics.getErrorCountPerStream(
                                streamId, topic, Integer.parseInt(partitionNo), e.getClass().getSimpleName()).inc();
                    }
                    if (e.getMessage().contains("apache.kafka.common.errors.TimeoutException")) {
                        throw new ConnectionUnavailableException("TimeoutException occurred when trying to send " +
                                "message. Broker may not be unavailable.", e);
                    }
                }
            } else {
                if (null == partitionNo) {
                    producer.send(new ProducerRecord<>(topic, null, System.currentTimeMillis(), key,
                                    payloadToSend),
                            new KafkaProducerCallBack(topic, null, System.currentTimeMillis()));
                } else {
                    Integer partition = Integer.parseInt(partitionNo);
                    long currentTime = System.currentTimeMillis();
                    producer.send(new ProducerRecord<>(topic, partition, currentTime,
                                    key, payloadToSend),
                            new KafkaProducerCallBack(topic, partition, currentTime));
                }
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("Error while converting the received string payload to byte[].", e);
        } catch (InterruptedException e) {
            LOG.error("Error when sending the event synchronously.", e);
        }
    }

    public void sendToMetrics(RecordMetadata metadata, long pushedTimestamp) {
        if (metrics != null) {
            metrics.getTotalWrites().inc();
            if (metadata != null) {
                String topic = metadata.topic();
                int partition = metadata.partition();

                metrics.getWriteCountPerStream(streamId, topic, partition).inc();
                metrics.getLastMessageSize(topic, partition, streamId, metadata.serializedValueSize());
                metrics.getLastMessageAckLatency(topic, partition, streamId,
                        metadata.timestamp() - pushedTimestamp);
                metrics.getLastMessagePublishedTime(topic, partition, streamId, pushedTimestamp);
                metrics.getLastCommittedOffset(topic, partition, streamId, metadata.offset());
            }
        }
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

        if (useAvroSerializer) {
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("schema.registry.url", schemaRegistry);
        } else if (!isBinaryMessage) {
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } else {
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        }

        KafkaIOUtils.splitHeaderValues(optionalConfigs, props);
        producer = new KafkaProducer<>(props);
        LOG.info("Kafka producer created.");
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
        return new Class[]{String.class, ByteBuffer.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{KAFKA_PUBLISH_TOPIC, KAFKA_PARTITION_NO, KAFKA_MESSAGE_KEY};
    }

    public byte[] getSequencedBinaryPayloadToSend(byte[] payload, KafkaSinkState kafkaSinkState) {
        StringBuilder strPayload = new StringBuilder();
        strPayload.append(sequenceId).append(SEQ_NO_HEADER_FIELD_SEPERATOR)
                .append(kafkaSinkState.lastSentSequenceNo.get())
                .append(SEQ_NO_HEADER_DELIMITER);
        int headerSize = strPayload.toString().length();
        int bufferSize = headerSize + 4 + payload.length;
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[bufferSize]);
        byteBuffer.putInt(headerSize);
        byteBuffer.put(strPayload.toString().getBytes(Charset.defaultCharset()));
        byteBuffer.put(payload);
        return byteBuffer.array();
    }

    private boolean useAvroSerializer(StreamDefinition outputStreamDefinition) {
        Iterator iterator = outputStreamDefinition.getAnnotations().iterator();
        Annotation sinkAnnotation = null;
        while (iterator.hasNext()) {
            Annotation annotation = (Annotation) iterator.next();
            if (annotation.getName().equalsIgnoreCase(SiddhiConstants.ANNOTATION_SINK)) {
                sinkAnnotation = annotation;
                break;
            }
        }
        if (sinkAnnotation == null) {
            throw new SiddhiAppCreationException(SiddhiConstants.ANNOTATION_SINK + " annotation not found in " +
                    outputStreamDefinition.getId() + ", in Siddhi app: " + siddhiAppName);
        }
        Iterator sinkAnnotationIterator = sinkAnnotation.getAnnotations().iterator();
        while (sinkAnnotationIterator.hasNext()) {
            Annotation annotation = (Annotation) sinkAnnotationIterator.next();
            if (annotation.getName().equalsIgnoreCase(SiddhiConstants.ANNOTATION_MAP)) {
                String type = annotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_TYPE);
                if (type != null && type.equalsIgnoreCase("avro")) {
                    schemaRegistry = annotation.getElement("schema.registry");
                    String useAvroStr = annotation.getElement("use.avro.serializer");
                    boolean useAvro = Boolean.parseBoolean(useAvroStr);
                    if (useAvro && (schemaRegistry == null || schemaRegistry.isEmpty())) {
                        throw new SiddhiAppCreationException("Property schema.registry is mandatory when " +
                                "use.avro.serializer property is set to true. In Kafka Sink for: " +
                                outputStreamDefinition.getId() + ", in Siddhi app: " + siddhiAppName);
                    }
                    return useAvro;
                }
            }
        }
        return false;
    }

    private class KafkaProducerCallBack implements Callback {

        //The time which the message was attempted to be submitted by the KafkaSink
        private long pushedTimestamp;
        private String topic;
        private Integer partition;


        KafkaProducerCallBack(String topic, Integer partition, long pushedTimestamp) {
            this.topic = topic;
            this.pushedTimestamp = pushedTimestamp;
            this.partition = partition;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metrics != null) {
                metrics.getTotalWrites().inc();

                if (metadata != null) {
                    String topic = metadata.topic();
                    int partition = metadata.partition();

                    metrics.getWriteCountPerStream(streamId, topic, partition).inc();
                    metrics.getLastMessageSize(topic, partition, streamId, metadata.serializedValueSize());
                    metrics.getLastMessageAckLatency(topic, partition, streamId,
                            metadata.timestamp() - pushedTimestamp);
                    metrics.getLastMessagePublishedTime(topic, partition, streamId, pushedTimestamp);
                    metrics.getLastCommittedOffset(topic, partition, streamId, metadata.offset());
                } else {  //error has occurred
                    metrics.getErrorCountWithoutPartition(
                            this.topic, streamId, exception.getClass().getSimpleName()).inc();
                    // When the siddhi app user has not given a partition, then this stat won't be published.
                    if (this.partition != null) {
                        metrics.getErrorCountPerStream(
                                streamId, this.topic, this.partition, exception.getClass().getSimpleName()).inc();
                    }
                }
            }
        }
    }

    /**
     * State class for Kafka sink.
     */
    public class KafkaSinkState extends State {
        public AtomicInteger lastSentSequenceNo = new AtomicInteger(0);
        private boolean isSequenced = false;

        KafkaSinkState(boolean isSequenced) {
            this.isSequenced = isSequenced;
        }

        @Override
        public Map<String, Object> snapshot() {
            if (isSequenced) {
                Map<String, Object> state = new HashMap<>();
                state.put(LAST_SENT_SEQ_NO_PERSIST_KEY, lastSentSequenceNo.get());
                return state;
            } else {
                return null;
            }
        }

        public void restore(Map<String, Object> state) {
            if (isSequenced) {
                Object sequenceNumber = state.get(LAST_SENT_SEQ_NO_PERSIST_KEY);
                if (sequenceNumber != null) {
                    lastSentSequenceNo.set((Integer) sequenceNumber);
                }
            }
        }

        @Override
        public boolean canDestroy() {
            return false;
        }
    }
}
