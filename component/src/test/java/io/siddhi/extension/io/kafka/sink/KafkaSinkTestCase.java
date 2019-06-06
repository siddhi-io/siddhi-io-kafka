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
 * Class implementing the Test cases for Kafka Sink.
 */
public class KafkaSinkTestCase {
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
    public static void stopKafkaBroker() throws InterruptedException {
        KafkaTestUtil.stopKafkaBroker();
        Thread.sleep(1000);
    }

    @BeforeMethod
    public void init2() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testPublisherWithTopicWithoutPartitionKafkaTransport() throws InterruptedException {
        LOG.info("Creating test for publishing events for static topic without a partition");
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
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
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
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"single_topic", 55.6f, 100L});
            fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
            fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
            Thread.sleep(3000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("single_topic");
            expectedNames.add("single_topic2");
            expectedNames.add("single_topic3");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(100L);
            expectedValues.add(102L);
            expectedValues.add(103L);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(3, count);
            KafkaTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(dependsOnMethods = "testPublisherWithTopicWithoutPartitionKafkaTransport")
    public void testPublisherWithTopicWithPartitionKafkaTransport() throws InterruptedException {
        LOG.info("Creating test for publishing events for static topic with a partition");
        String topics[] = new String[]{"topic_with_two_partitions_sub0"};
        KafkaTestUtil.createTopic(topics, 2);
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan2') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='topic_with_two_partitions_sub0', partition.no.list='0',"
                            + "group.id='topic_with_two_partitions_sub0_test', threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
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
                    "@App:name('TestExecutionPlan3') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='topic_with_two_partitions_sub0', partition.no='0', "
                            + "bootstrap.servers='localhost:9092', " +
                            "@map(type='xml'))" +
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
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(3, count);
            KafkaTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(dependsOnMethods = "testPublisherWithTopicWithPartitionKafkaTransport")
    public void testPublisherWithInvalidTopicKafkaTransport() throws InterruptedException {
        LOG.info("Creating test for publishing events for invalid topic without a partition");
        String topics[] = new String[]{"invalid_topic_without_partition2"};
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan4') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='invalid_topic_without_partition2', "
                            + "bootstrap.servers='localhost:9092', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            //this event will be published to the cluster and create the topic
            fooStream.send(new Object[]{"invalid_topic_without_partition", 55.6f, 100L});
            //this thread sleep is to slowdown the below execution plan because, if there are no topics kafka source
            // will create the topic.
            Thread.sleep(500);
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan5') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='invalid_topic_without_partition2', "
                            + "group.id='invalid_topic_without_partition2_test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
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
            fooStream.send(new Object[]{"invalid_topic_without_partition2", 75.6f, 102L});
            fooStream.send(new Object[]{"invalid_topic_without_partition3", 57.6f, 103L});
            Thread.sleep(2000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("invalid_topic_without_partition");
            expectedNames.add("invalid_topic_without_partition2");
            expectedNames.add("invalid_topic_without_partition3");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(100L);
            expectedValues.add(102L);
            expectedValues.add(103L);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(3, count);
            KafkaTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(dependsOnMethods = "testPublisherWithInvalidTopicKafkaTransport")
    public void testPublisherWithInvalidTopicWithPartitionKafkaTransport() throws InterruptedException {
        LOG.info("Creating test for publishing events for invalid topic with a partition");
        String topics[] = new String[]{"invalid_topic_with_partition"};
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan6') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='invalid_topic_with_partition', "
                            + "bootstrap.servers='localhost:9092', partition.no='0', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            //this event will be published to the cluster and create the topic
            fooStream.send(new Object[]{"invalid_topic_with_partition", 55.6f, 100L});
            //this thread sleep is to slowdown the below execution plan because, if there are no topics kafka source
            // will create the topic.
            Thread.sleep(500);
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan7') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='invalid_topic_with_partition', "
                            + "group.id='invalid_topic_with_partition_test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092', "
                            + "partition.no.list='0', " +
                            "@map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
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
            fooStream.send(new Object[]{"invalid_topic_with_partition2", 75.6f, 102L});
            fooStream.send(new Object[]{"invalid_topic_with_partition3", 57.6f, 103L});
            Thread.sleep(2000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("invalid_topic_with_partition");
            expectedNames.add("invalid_topic_with_partition2");
            expectedNames.add("invalid_topic_with_partition3");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(100L);
            expectedValues.add(102L);
            expectedValues.add(103L);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(3, count);
            KafkaTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(dependsOnMethods = "testPublisherWithInvalidTopicWithPartitionKafkaTransport")
    public void testPublisherWithInvalidTopicWithPartitionOtherThan0KafkaTransportReceiveMessage() throws
            InterruptedException {
        LOG.info("Creating test for publishing events for invalid topic with a partition other than 0 but the source "
                + "will be getting events from the default partition when given a key");
        String topics[] = new String[]{"invalid_topic_with_partition_3"};
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan8') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='invalid_topic_with_partition_3', "
                            + "bootstrap.servers='localhost:9092', key='{{volume}}', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            //this event will be published to the cluster and create the topic
            fooStream.send(new Object[]{"invalid_topic_with_partition_2", 55.6f, 100L});
            //this thread sleep is to slowdown the below execution plan because, if there are no topics kafka source
            // will create the topic.
            Thread.sleep(500);
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan9') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='invalid_topic_with_partition_3', "
                            + "group.id='invalid_topic_with_partition_3_test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092', " +
                            "@map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
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
            fooStream.send(new Object[]{"invalid_topic_with_partition_22", 75.6f, 102L});
            fooStream.send(new Object[]{"invalid_topic_with_partition_23", 57.6f, 103L});
            Thread.sleep(2000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("invalid_topic_with_partition_2");
            expectedNames.add("invalid_topic_with_partition_22");
            expectedNames.add("invalid_topic_with_partition_23");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(100L);
            expectedValues.add(102L);
            expectedValues.add(103L);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Sink didnt publish the expected events", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(3, count);
            KafkaTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test (dependsOnMethods = "testPublisherWithInvalidTopicWithPartitionOtherThan0KafkaTransportReceiveMessage")
    public void testPublisherWithInvalidTopicWithPartitionOtherThan0KafkaTransport() throws InterruptedException {
        LOG.info("Creating test for publishing events for invalid topic with a partition other than 0 but the source "
                + "will be not be getting events from the default partition since the key is not defined");
        String topics[] = new String[]{"invalid_topic_with_partition_2"};
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan10') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='invalid_topic_with_partition_2', "
                            + "bootstrap.servers='localhost:9092', partition.no='2', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            //this event will be published to the cluster and create the topic
            fooStream.send(new Object[]{"invalid_topic_with_partition_2", 55.6f, 100L});
            //this thread sleep is to slowdown the below execution plan because, if there are no topics kafka source
            // will create the topic.
            Thread.sleep(2000);
            SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan11') " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='invalid_topic_with_partition_2', "
                            + "group.id='invalid_topic_with_partition_2_test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092', " +
                            "@map(type='xml'))" +
                            "Define stream FooStream2 (symbol string, price float, volume long);" +
                            "from FooStream2 select symbol, price, volume insert into BarStream2;");
            siddhiAppRuntimeSource.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
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
            Thread.sleep(2000);
            fooStream.send(new Object[]{"invalid_topic_with_partition_22", 75.6f, 102L});
            fooStream.send(new Object[]{"invalid_topic_with_partition_23", 57.6f, 103L});
            Thread.sleep(2000);
            AssertJUnit.assertEquals(0, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test (dependsOnMethods = "testPublisherWithInvalidTopicWithPartitionOtherThan0KafkaTransport")
    @Test(dependsOnMethods =
            "testPublisherWithInvalidTopicWithPartitionOtherThan0KafkaTransportReceiveMessage")
    public void testPublisherWithKafkaTransportWithDynamicTopic() throws InterruptedException {
        LOG.info("Creating test for publishing events for dynamic topic without partition");
        try {
            String topics[] = new String[]{"multiple_topic1", "multiple_topic2"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 1);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan12') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='{{symbol}}', bootstrap.servers='localhost:9092', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream; ");
            InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");
            executionPlanRuntime.start();


            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan13') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='multiple_topic1,multiple_topic2', "
                            + "group.id='multiple_topic1_multiple_topic2_test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        LOG.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"multiple_topic1", 55.6f, 100L});
            fooStream.send(new Object[]{"multiple_topic2", 75.6f, 102L});
            Thread.sleep(2000);

            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1");
            expectedNames.add("multiple_topic2");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(100L);
            expectedValues.add(102L);
            AssertJUnit.assertEquals(2, count);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            KafkaTestUtil.deleteTopic(topics);
            executionPlanRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test
    public void testPublisherWithKafkaTransportWithDynamicTopicAndPartition() throws InterruptedException {
        LOG.info("Creating test for publishing events for dynamic topic with dynamic partition");
        try {
            String topics[] = new String[]{"multiple_topic1_two_par_all_sub1", "multiple_topic2_two_par_all_sub1"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(10000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan14') " +
                            "define stream FooStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type='kafka', topic='{{symbol}}', partition.no='{{volume}}', "
                            + "bootstrap.servers='localhost:9092', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream; ");
            InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");
            executionPlanRuntime.start();


            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan15') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='multiple_topic1_two_par_all_sub1,multiple_topic2_two_par_all_sub1', "
                            + "group.id='multiple_topic1_two_par_all_sub1_test', threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092', "
                            + "partition.no.list='0,1'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        LOG.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            fooStream.send(new Object[]{"multiple_topic1_two_par_all_sub1", 55.6f, 0L});
            Thread.sleep(1000);
            fooStream.send(new Object[]{"multiple_topic1_two_par_all_sub1", 55.6f, 1L});
            Thread.sleep(1000);
            fooStream.send(new Object[]{"multiple_topic2_two_par_all_sub1", 75.6f, 0L});
            Thread.sleep(1000);
            fooStream.send(new Object[]{"multiple_topic2_two_par_all_sub1", 75.6f, 1L});
            Thread.sleep(5000);

            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1_two_par_all_sub1");
            expectedNames.add("multiple_topic1_two_par_all_sub1");
            expectedNames.add("multiple_topic2_two_par_all_sub1");
            expectedNames.add("multiple_topic2_two_par_all_sub1");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            expectedValues.add(1L);
            expectedValues.add(0L);
            expectedValues.add(1L);
            AssertJUnit.assertEquals(4, count);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                    receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            KafkaTestUtil.deleteTopic(topics);
            executionPlanRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }
}
