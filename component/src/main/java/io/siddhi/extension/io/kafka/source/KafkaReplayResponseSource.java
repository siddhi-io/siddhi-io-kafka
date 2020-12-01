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

@Extension(
        name = "kafka-replay-response",
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
                @Parameter(name = "enable.offsets.commit",
                        description = "This parameter specifies whether to commit offsets. \n"
                                + "If the manual asynchronous offset committing is needed, `enable.offsets.commit` "
                                + "should be `true` and `enable.auto.commit` should be `false`. \n"
                                + "If periodical committing is needed `enable.offsets.commit` should be `true` and "
                                + "`enable.auto.commit` should be `true`. \n"
                                + "If committing is not needed, `enable.offsets.commit` should be `false`. \n"
                                + "\n"
                                + "Note: `enable.auto.commit` is an `optional.configuration` property. If it is set to "
                                + "`true`, Source will periodically(default: 1000ms. Configurable with "
                                + "`auto.commit.interval.ms` property as an `optional.configuration`) commit its "
                                + "current offset (defined as the offset of the next message to be read) "
                                + "for the partitions it is reading from back to Kafka. \n"
                                + "To guarantee at-least-once processing, we recommend you to enable "
                                + "Siddhi Periodic State Persistence when `enable.auto.commit` property "
                                + "is set to `true`. \n"
                                + "During manual committing, it might introduce a latency during consumption.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(name = "enable.async.commit",
                        description = "This parameter will changes the type of the committing offsets returned on " +
                                "the last poll for the subscribed list of topics and partitions.\n" +
                                "When `enable.async.commit` is set to true, committing will be an asynchronous call.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
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
                                " to a Siddhi event, and sent to a stream named `FooStream`."),
                @Example(
                        syntax = "@App:name('TestExecutionPlan')\n" +
                                "@source(type='kafka',\n" +
                                "        topic.list='trp_topic',\n" +
                                "        partition.no.list='0',\n" +
                                "        threading.option='single.thread',\n" +
                                "        group.id='group',\n" +
                                "        bootstrap.servers='localhost:9092',\n" +
                                "        @map(type='xml', enclosing.element='//events', " +
                                "            @attributes(symbol ='symbol', price = 'price', volume = 'volume', " +
                                "                        partition = 'trp:partition', " +
                                "                        topic = 'trp:topic', key = 'trp:key', " +
                                "                        recordTimestamp = 'trp:record.timestamp',  " +
                                "                        eventTimestamp = 'trp:event.timestamp', " +
                                "                        checkSum = 'trp:check.sum', topicOffset = 'trp:offset')))\n" +
                                "define stream FooStream (symbol string, price float, volume long, " +
                                "                                     partition string, " +
                                "                                     topic string, key string, " +
                                "                                     recordTimestamp string, " +
                                "                                     eventTimestamp string, checkSum string, " +
                                "                                     topicOffset string);\n" +
                                "from FooStream select * insert into BarStream;",
                        description = "This Kafka source configuration listens to the `trp_topic` topic for the " +
                                "default partition because no `partition.no.list` is defined. \n" +
                                "Since the custom attribute mapping is enabled with TRP values, the siddhi event " +
                                "will be populated with the relevant trp values as well")
        }
)

public class KafkaReplayResponseSource extends KafkaSource {
}
