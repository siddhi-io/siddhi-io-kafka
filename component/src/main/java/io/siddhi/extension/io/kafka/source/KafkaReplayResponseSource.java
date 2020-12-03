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
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.kafka.Constants;
import io.siddhi.extension.io.kafka.KafkaIOUtils;
import io.siddhi.extension.io.kafka.util.KafkaReplayResponseSourceRegistry;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.kafka.source.KafkaSource.ADAPTOR_SUBSCRIBER_TOPIC;

@Extension(
        name = "kafka-replay-response",
        namespace = "source",
        description = "",
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
                        syntax = "",
                        description = "")
        }
)

public class KafkaReplayResponseSource extends Source {
    private String sinkID;
    private SourceEventListener sourceEventListener;
    private String[] requestedTransportPropertyNames;
    private boolean paused;
    private ReentrantLock lock;
    private Condition condition;
    private static final Logger logger = Logger.getLogger(KafkaReplayResponseSource.class.getName());
    private String[] topics;

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link io.siddhi.query.api.SiddhiApp} used to get Siddhi
     */
    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                             String[] requestedTransportPropertyNames, ConfigReader configReader,
                             SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
        sinkID = optionHolder.validateAndGetOption(Constants.ID).getValue();
        KafkaReplayResponseSourceRegistry.getInstance().putKafkaReplayResponseSource(sinkID, this);
        lock = new ReentrantLock();
        condition = lock.newCondition();
        String topicList = optionHolder.validateAndGetStaticValue(ADAPTOR_SUBSCRIBER_TOPIC);
        topics = topicList.split(KafkaIOUtils.HEADER_SEPARATOR);
        return null;
    }
//
//    public void onResponse(Event response, Map<String, String> siddhiRequestEventData) {
//        handlePause();
//        sourceEventListener.onEvent(response.getPayload(), getTransportProperties(response.getHeadersMap(),
//                siddhiRequestEventData));
//    }
    public void onResponse(Object response, Map<String, String> siddhiRequestEventData) {
        handlePause();
        sourceEventListener.onEvent(response, getTransportProperties(siddhiRequestEventData));
    }

    public void onReplayRequest() {
        KafkaConsumerThread kafkaConsumerThread =
                new KafkaConsumerThread(sourceEventListener, topics, partitions, props,
                        false, isBinaryMessage, enableOffsetCommit, enableAsyncCommit,
                        requiredProperties);
    }

    private String[] getTransportProperties(Map<String, String> headersMap,
                                            Map<String, String> siddhiRequestEventData) {
        siddhiRequestEventData.putAll(headersMap);
        String[] transportProperties = new String[requestedTransportPropertyNames.length];
        for (int i = 0; i < requestedTransportPropertyNames.length; i++) {
            if (siddhiRequestEventData.containsKey(requestedTransportPropertyNames[i])) {
                transportProperties[i] = siddhiRequestEventData.get(requestedTransportPropertyNames[i]);
            }
        }
        return transportProperties;
    }
    private String[] getTransportProperties(Map<String, String> siddhiRequestEventData) {
        String[] transportProperties = new String[requestedTransportPropertyNames.length];
        for (int i = 0; i < requestedTransportPropertyNames.length; i++) {
            if (siddhiRequestEventData.containsKey(requestedTransportPropertyNames[i])) {
                transportProperties[i] = siddhiRequestEventData.get(requestedTransportPropertyNames[i]);
            }
        }
        return transportProperties;
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, State state) throws ConnectionUnavailableException {

    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {

    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}.
     */
    @Override
    public void destroy() {
        KafkaReplayResponseSourceRegistry.getInstance().removeKafkaReplayResponseSource(sinkID);
    }

    /**
     * Called to pause event consumption.
     */
    @Override
    public void pause() {
        lock.lock();
        try {
            paused = true;
            logger.info("Response has pause for grpc-call-response source with sink.id: " + sinkID);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called to resume event consumption.
     */
    @Override
    public void resume() {
        lock.lock();
        try {
            paused = false;
            logger.info("Response has resume for grpc-call-response source with sink.id: " + sinkID);
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }


    private void handlePause() {
        if (paused) {
            lock.lock();
            try {
                while (paused) {
                    condition.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread interrupted while pausing ", e);
            } finally {
                lock.unlock();
            }
        }
    }
}
