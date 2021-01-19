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
package io.siddhi.extension.io.kafka.sink;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.kafka.Constants;
import io.siddhi.extension.io.kafka.util.KafkaReplayResponseSourceRegistry;
import io.siddhi.query.api.definition.StreamDefinition;

/**
 * This class implements a Kafka Replay Request Sink
 */
@Extension(
        name = "kafka-replay-request",
        namespace = "sink",
        description = "This sink is used to request replay of specific range of events on a specified partition of a " +
                "topic.",
        parameters = {
                @Parameter(name = "sink.id",
                        description = "a unique SINK_ID should be set. This sink id will be used to match with the " +
                                "appropriate kafka-replay-response source",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@App:name('TestKafkaReplay')\n" +
                                "\n" +
                                "@sink(type='kafka-replay-request', sink.id='1')\n" +
                                "define stream BarStream (topicForReplay string, partitionForReplay string, " +
                                "startOffset string, endOffset string);\n" +
                                "\n" +
                                "@info(name = 'query1')\n" +
                                "@source(type='kafka-replay-response',  group.id='group', threading.option=" +
                                "'single.thread', bootstrap.servers='localhost:9092', sink.id='1',\n" +
                                "@map(type='json'))\n" +
                                "Define stream FooStream (symbol string, amount double);\n" +
                                "\n" +
                                "@sink(type='log')\n" +
                                "Define stream logStream(symbol string, amount double);\n" +
                                "\n" +
                                "from FooStream select * insert into logStream;",
                        description = "In this app we can send replay request events into BarStream and observe the " +
                                "replayed events in the logStream")
        }
)

public class KafkaReplayRequestSink extends Sink {
    private String sinkID;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Event.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    protected StateFactory init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        this.sinkID = optionHolder.validateAndGetOption(Constants.SINK_ID).getValue();
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        String partitionForReplay;
        String startOffset;
        String endOffset;
        String replayTopic;
        Object[] replayParams;
        if (payload instanceof Event[]) {
            replayParams = ((Event[]) payload)[0].getData();
        } else if (payload instanceof Event) {
            replayParams = ((Event) payload).getData();
        } else {
            throw new ConnectionUnavailableException("Unknown type");
        }
        replayTopic = (String) replayParams[0];
        partitionForReplay = (String) replayParams[1];
        startOffset = (String) replayParams[2];
        endOffset = (String) replayParams[3];
        KafkaReplayResponseSourceRegistry.getInstance().getKafkaReplayResponseSource(sinkID)
                .onReplayRequest(partitionForReplay, startOffset, endOffset, replayTopic);
    }

    @Override
    public void connect() throws ConnectionUnavailableException {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void destroy() {

    }
}
