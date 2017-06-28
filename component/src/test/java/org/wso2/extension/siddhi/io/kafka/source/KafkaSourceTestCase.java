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

import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.kafka.KafkaTestUtil;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;


public class KafkaSourceTestCase {
    private static final Logger log = Logger.getLogger(KafkaSourceTestCase.class);
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
    public void testKafkaSingleTopicSource_53_103() throws InterruptedException {
        try {
            log.info("Creating test for single topic");
            String topics[] = new String[]{"single_topic"};
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            KafkaTestUtil.createTopic(topics, 1);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='single_topic', group.id='test_single_topic', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 2, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("single_topic");
            expectedNames.add("single_topic");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            expectedValues.add(1L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(2, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class,
          dependsOnMethods = "testKafkaSingleTopicSource_53_103")
    public void testKafkaWithoutTopicSource_54() {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for without topic");
            log.info("-------------------------------------------------------------------------------------------");
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', group.id='test', threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaWithoutTopicSource_54")
    public void testKafkaMultipleTopicSource_85_98() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for multiple topic");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"multiple_topic1", "multiple_topic2"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 1);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='multiple_topic1,multiple_topic2', "
                            + "group.id='test_multiple_topic1_multiple_topic2', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 2, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1");
            expectedNames.add("multiple_topic1");
            expectedNames.add("multiple_topic2");
            expectedNames.add("multiple_topic2");
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
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaMultipleTopicSource_85_98")
    public void testKafkaSingleTopicWithSpecificSubscribeSource_92() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for single topic with one partition which subscribes for the partition "
                             + "specifically");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"topic_with_one_partition"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 1);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='topic_with_one_partition', "
                            + "partition.no.list='0', "
                            + "group.id='test_topic_with_one_partition', "
                            + "threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 1, true, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("topic_with_one_partition");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(1, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class,
          dependsOnMethods = "testKafkaSingleTopicWithSpecificSubscribeSource_92")
    public void testKafkaSpecificSubscribeForUnavailablePartitionSource_105_120() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for single topic with partitions which subscribes for an unavailable partition.");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"topic_without_some_partition"};
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='topic_without_some_partition', "
                            + "partition.no.list='0,1,2', "
                            + "group.id='test_topic_without_some_partition', threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaSpecificSubscribeForUnavailablePartitionSource_105_120")
    public void testKafkaMultipleTopic_MultiplePartition_OnePartitionSubscribe_Source_100() throws
                                                                                            InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test to configure Kafka source with multiple topics having multiple partitions "
                             + "subscribing for single partition id");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"multiple_topic1_two_par_one_sub", "multiple_topic2_two_par_one_sub"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='multiple_topic1_two_par_one_sub,multiple_topic2_two_par_one_sub', "
                            + "partition.no.list='1', "
                            + "group.id='test_multiple_topic1_two_par_one_sub', "
                            + "threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 2, 2, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1_two_par_one_sub");
            expectedNames.add("multiple_topic2_two_par_one_sub");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(1L);
            expectedValues.add(1L);
            AssertJUnit.assertEquals(2, count);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaMultipleTopic_MultiplePartition_OnePartitionSubscribe_Source_100")
    public void testKafkaMultipleTopic_MultiplePartition_AllPartitionSubscribe_Source_101() throws
                                                                                            InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test to configure Kafka source with multiple topics having multiple partitions "
                             + "subscribing for all partition ids");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"multiple_topic1_two_par_all_sub", "multiple_topic2_two_par_all_sub"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='multiple_topic1_two_par_all_sub,multiple_topic2_two_par_all_sub', "
                            + "partition.no.list='0,1', "
                            + "group.id='test_multiple_topic1_two_par_all_sub', "
                            + "threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 2, 2, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1_two_par_all_sub");
            expectedNames.add("multiple_topic1_two_par_all_sub");
            expectedNames.add("multiple_topic2_two_par_all_sub");
            expectedNames.add("multiple_topic2_two_par_all_sub");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            expectedValues.add(1L);
            expectedValues.add(0L);
            expectedValues.add(1L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(4, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class,
          dependsOnMethods = "testKafkaMultipleTopic_MultiplePartition_AllPartitionSubscribe_Source_101")
    public void testKafkaWithoutBootstrapServerSource_108() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for without any bootstrap servers defined.");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"no_bootstrap_server_topic"};
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='no_bootstrap_server_topic', partition.no.list='0,1,2', "
                            + "group.id='test', threading.option='single.thread'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaWithoutBootstrapServerSource_108")
    public void testKafkaMultipleTopicWithThreadingPerTopicSource_111() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for multiple topic with thread per topic");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"multiple_topic1", "multiple_topic2"};
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            KafkaTestUtil.createTopic(topics, 1);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='multiple_topic1,multiple_topic2', "
                            + "group.id='test_multiple_topic1_multiple_topic2', "
                            + "threading.option='topic.wise', "
                            + "bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 2, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1");
            expectedNames.add("multiple_topic1");
            expectedNames.add("multiple_topic2");
            expectedNames.add("multiple_topic2");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            expectedValues.add(1L);
            expectedValues.add(0L);
            expectedValues.add(1L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(4, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaMultipleTopicWithThreadingPerTopicSource_111")
    public void testKafkaMultipleTopicWithThreadingPerPartitionSource_112() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for multiple topic with thread per partition");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"multiple_topic1_2par", "multiple_topic2_2par"};
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='multiple_topic1_2par,multiple_topic2_2par', "
                            + "group.id='test_multiple_topic1_2par_multiple_topic2_2par', "
                            + "threading.option='partition.wise', "
                            + "bootstrap.servers='localhost:9092', "
                            + "partition.no.list='0,1'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 2, 2, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("multiple_topic1_2par");
            expectedNames.add("multiple_topic1_2par");
            expectedNames.add("multiple_topic2_2par");
            expectedNames.add("multiple_topic2_2par");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            expectedValues.add(1L);
            expectedValues.add(0L);
            expectedValues.add(1L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(4, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class,
          dependsOnMethods = "testKafkaMultipleTopicWithThreadingPerPartitionSource_112")
    public void testKafkaWithoutThreadingOptionSource_113() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for without any threading option defined.");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"no_threading_option_topic"};
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='no_threading_option_topic', partition.no.list='0,1,2', "
                            + "group.id='test_no_threading_option_topic'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class,
          dependsOnMethods = "testKafkaWithoutThreadingOptionSource_113")
    public void testKafkaSingleTopicWithoutGroupIdSource_114_129_143() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for single topic without group ID");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"single_topic_without_groupid"};
            KafkaTestUtil.createTopic(topics, 1);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='single_topic_without_groupid', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");

            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaSingleTopicWithoutGroupIdSource_114_129_143")
    public void testKafkaSingleTopicDifferentGroupIdsSource_133() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for single topic subscribed by multiple sources with different group ids");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"single_topic_different_group_ids"};
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            KafkaTestUtil.createTopic(topics, 1);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +

                            "@info(name = 'query1') "
                            + "@source(type='kafka', topic.list='single_topic_different_group_ids', "
                            + "group.id='test_single_topic_different_group_ids', "
                            + "threading.option='single.thread', bootstrap.servers='localhost:9092',"
                            + "@map(type='xml'))"
                            + "Define stream FooStream (symbol string, price float, volume long); " +

                            "@info(name = 'query2') "
                            + "@source(type='kafka', topic.list='single_topic_different_group_ids', "
                            + "group.id='test2_single_topic_different_group_ids2', "
                            + "threading.option='single.thread', bootstrap.servers='localhost:9092',"
                            + "@map(type='xml'))"
                            + "Define stream FooStream2 (symbol string, price float, volume long); " +

                            "from FooStream select symbol, price, volume insert into BarStream; " +
                            "from FooStream2 select symbol, price, volume insert into BarStream2; ");

            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 1, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("single_topic_different_group_ids");
            expectedNames.add("single_topic_different_group_ids");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            expectedValues.add(0L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(2, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaSingleTopicDifferentGroupIdsSource_133")
    public void testKafkaSingleTopicSameGroupIdsSource_140() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for single topic subscribed by multiple sources with same group ids");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"single_topic_same_group_ids"};
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            KafkaTestUtil.createTopic(topics, 1);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "define stream BarStream2 (symbol string, price float, volume long); " +

                            "@info(name = 'query1') "
                            + "@source(type='kafka', topic.list='single_topic_same_group_ids', "
                            + "group.id='test_single_topic_same_group_ids', "
                            + "threading.option='single.thread', bootstrap.servers='localhost:9092',"
                            + "@map(type='xml'))"
                            + "Define stream FooStream (symbol string, price float, volume long); " +

                            "@info(name = 'query2') "
                            + "@source(type='kafka', topic.list='single_topic_same_group_ids', "
                            + "group.id='test_single_topic_same_group_ids', "
                            + "threading.option='single.thread', bootstrap.servers='localhost:9092',"
                            + "@map(type='xml'))"
                            + "Define stream FooStream2 (symbol string, price float, volume long); " +

                            "from FooStream select symbol, price, volume insert into BarStream; " +
                            "from FooStream2 select symbol, price, volume insert into BarStream2; ");

            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 1, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("single_topic_same_group_ids");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            AssertJUnit.assertEquals(1, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaSingleTopicSameGroupIdsSource_140")
    public void testKafkaNonExistingTopicSource_121_123() throws InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test for non-existing topic. This will create a topic with default partition");
            log.info("-------------------------------------------------------------------------------------------");
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            String topics[] = new String[]{"non_existing_topic1"};
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='non_existing_topic1', "
                            + "group.id='test_non_existing_topic1', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 1, 1, false, null, true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("non_existing_topic1");
            List<Long> expectedValues = new ArrayList<>(2);
            expectedValues.add(0L);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
            KafkaTestUtil.deleteTopic(topics);
            AssertJUnit.assertEquals(1, count);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test (dependsOnMethods = "testKafkaNonExistingTopicSource_121_123")
    public void testKafkaMultipleTopicPartitionPartitionWiseSubscription() throws InterruptedException {
        log.info("-------------------------------------------------------------------------------------------");
        log.info("Creating test for multiple topics and partitions and thread partition wise");
        log.info("-------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(4);
        receivedValueList = new ArrayList<>(4);
        String topics[] = new String[]{"kafka_topic", "kafka_topic2"};
        KafkaTestUtil.createTopic(topics, 2);
        Thread.sleep(4000);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream BarStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='kafka', topic.list='kafka_topic,kafka_topic2', group.id='test', " +
                        "threading.option='partition.wise', bootstrap.servers='localhost:9092', " +
                        "partition.no.list='0,1', " +
                        "@map(type='xml'))" +
                        "Define stream FooStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                    receivedEventNameList.add(event.getData(0).toString());
                    receivedValueList.add((long) event.getData(2));
                }

            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(2000);
        KafkaTestUtil.kafkaPublisher(topics, 2, 2, false, null, true);
        Thread.sleep(1000);
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        List<String> expectedNames = new ArrayList<>(2);
        expectedNames.add("kafka_topic");
        expectedNames.add("kafka_topic");
        expectedNames.add("kafka_topic2");
        expectedNames.add("kafka_topic2");
        List<Long> expectedValues = new ArrayList<>(2);
        expectedValues.add(0L);
        expectedValues.add(1L);
        expectedValues.add(0L);
        expectedValues.add(1L);
        AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                 receivedEventNameList);
        AssertJUnit.assertEquals("Kafka Source expected input not received", expectedValues, receivedValueList);
        KafkaTestUtil.deleteTopic(topics);
        siddhiAppRuntime.shutdown();
    }

    @Test (dependsOnMethods = "testKafkaMultipleTopicPartitionPartitionWiseSubscription")
    public void testKafkaMultipleTopicPartitionTopicWiseSubscription() throws InterruptedException {
        log.info("-------------------------------------------------------------------------------------------");
        log.info("Creating test for multiple topics and partitions and thread topic wise");
        log.info("-------------------------------------------------------------------------------------------");
        try {
            String topics[] = new String[]{"kafka_topic_with_2par", "kafka_topic2_with_2par"};
            receivedEventNameList = new ArrayList<>(4);
            receivedValueList = new ArrayList<>(4);
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(4000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='kafka_topic_with_2par,kafka_topic2_with_2par', "
                            + "group.id='test_kafka_topic_with_2par_kafka_topic2_with_2par', "
                            + "threading.option='topic.wise', bootstrap.servers='localhost:9092', "
                            + "partition.no.list='0,1', " +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 2, 2, false, null, true);
            Thread.sleep(1000);
            AssertJUnit.assertEquals(4, count);
            AssertJUnit.assertTrue(eventArrived);
            List<String> expectedNames = new ArrayList<>(4);
            expectedNames.add("kafka_topic_with_2par");
            expectedNames.add("kafka_topic_with_2par");
            expectedNames.add("kafka_topic2_with_2par");
            expectedNames.add("kafka_topic2_with_2par");
            List<Long> expectedValues = new ArrayList<>(4);
            expectedValues.add(0L);
            expectedValues.add(1L);
            expectedValues.add(0L);
            expectedValues.add(1L);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

//    @Test
    public void testKafkaSingleTopic_MultiplePartition_AllPartitionSubscribe_Source_106() throws
                                                                                          InterruptedException {
        try {
            log.info("-------------------------------------------------------------------------------------------");
            log.info("Creating test to configure Kafka source with multiple topics having multiple partitions "
                             + "subscribing for all partition ids");
            log.info("-------------------------------------------------------------------------------------------");
            String topics[] = new String[]{"single_topic1_two_par_two_servers"};
            KafkaTestUtil.setupKafkaBroker2();
            receivedEventNameList = new ArrayList<>(2);
            receivedValueList = new ArrayList<>(2);
            Thread.sleep(10000);
            KafkaTestUtil.createTopic(topics, 2);
            Thread.sleep(5000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', "
                            + "topic.list='single_topic1_two_par_two_servers', "
                            + "partition.no.list='0,1', "
                            + "group.id='test', "
                            + "threading.option='single.thread', "
                            + "bootstrap.servers='localhost:9092,localhost:9093'," +
                            "@map(type='xml'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                        receivedEventNameList.add(event.getData(0).toString());
                        receivedValueList.add((long) event.getData(2));
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            KafkaTestUtil.kafkaPublisher(topics, 2, 2, false, "localhost:9093,localhost:9092", true);
            Thread.sleep(1000);
            List<String> expectedNames = new ArrayList<>(2);
            expectedNames.add("single_topic1_two_par_two_servers");
            expectedNames.add("single_topic1_two_par_two_servers");
            expectedNames.add("single_topic1_two_par_two_servers");
            expectedNames.add("single_topic1_two_par_two_servers");
            List<Long> expectedValues = new ArrayList<>(2);
            AssertJUnit.assertEquals("Kafka Source expected input not received", expectedNames,
                                     receivedEventNameList);
            AssertJUnit.assertEquals(2, count);
            KafkaTestUtil.deleteTopic(topics);
            Thread.sleep(1000);
            KafkaTestUtil.stopKafkaBroker2();
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }
}

