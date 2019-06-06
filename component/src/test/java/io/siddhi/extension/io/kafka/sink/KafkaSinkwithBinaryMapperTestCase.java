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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.kafka.KafkaTestUtil;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test Class Implementing send message via binary mapping.
 */
public class KafkaSinkwithBinaryMapperTestCase {
    static final Logger LOG = Logger.getLogger(KafkaSinkTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private volatile List<String> receivedEventNameList;
    private volatile List<Long> receivedValueList;

    @BeforeClass
    public static void init() throws Exception {
        try {
            KafkaTestUtil.cleanLogDir();
            KafkaTestUtil.setupKafkaBroker();
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RemoteException("Exception caught when starting server", e);
        }
    }

    @AfterClass
    public static void stopKafkaBroker() {
        KafkaTestUtil.stopKafkaBroker();
    }

    @BeforeMethod
    public void init2() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testPublisherUsingBinaryMapper() throws InterruptedException {
        LOG.info("Creating test for publishing events using binary mapper.");
        String topics[] = new String[]{"single_topic"};
        KafkaTestUtil.createTopic(topics, 1);
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan1') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='single_topic', group.id='single_topic_test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092', " +
                            "is.binary.message='true'," +
                            "@map(type='binary'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                    for (Event event : events) {
                        LOG.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntimeSource.start();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='single_topic', bootstrap.servers='localhost:9092', " +
                            "is.binary.message = 'true'," +
                            "@map(type='binary'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"single_topic", 55.6f, 100L});
            fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
            fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
            Thread.sleep(2000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("single_topic");
            expectedNames.add("single_topic2");
            expectedNames.add("single_topic3");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(100L);
            expectedValues.add(102L);
            expectedValues.add(103L);
            AssertJUnit.assertEquals("Kafka Sink  published the expected events", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Sink published the expected events", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(3, count);
            KafkaTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }
}
