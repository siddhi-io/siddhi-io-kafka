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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.extension.io.kafka.KafkaTestUtil;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Variable;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Class implementing the Test cases for KafkaSourceHA Test Cases.
 */
public class KafkaSourceHATestCase {
    private static final Logger log = Logger.getLogger(KafkaSourceHATestCase.class);
    private static ExecutorService executorService;
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeClass
    public static void init() throws Exception {
        try {
            executorService = Executors.newFixedThreadPool(5);
            KafkaTestUtil.cleanLogDir();
            KafkaTestUtil.setupKafkaBroker();
            Thread.sleep(10000);
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
    public void testAKafkaPauseAndResume() throws InterruptedException {
        try {
            log.info("Test to verify the pause and resume functionality of Kafka source");
            String topics[] = new String[]{"kafka_topic3"};
            KafkaTestUtil.createTopic(topics, 2);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic.list='kafka_topic3', group.id='test1', threading" +
                            ".option='partition.wise', " +
                            "bootstrap.servers='localhost:9092', partition.no.list='0,1', " +
                            "@map(type='text'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                    }

                }
            });
            siddhiAppRuntime.start();
            Future eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    KafkaTestUtil.kafkaPublisher(topics, 2, 4, false, null, false);
                }
            });
            while (!eventSender.isDone()) {
                Thread.sleep(1000);
            }
            Thread.sleep(2000);
            assertEquals(4, count);
            assertTrue(eventArrived);

            Collection<List<Source>> sources = siddhiAppRuntime.getSources();
            // pause the transports
            sources.forEach(e -> e.forEach(Source::pause));

            init2();
            eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    KafkaTestUtil.kafkaPublisher(topics, 2, 4, false, null, false);
                }
            });
            while (!eventSender.isDone()) {
                Thread.sleep(1000);
            }
            Thread.sleep(5000);
            assertFalse(eventArrived);

            // resume the transports
            sources.forEach(e -> e.forEach(Source::resume));
            Thread.sleep(2000);
            assertEquals(4, count);
            assertTrue(eventArrived);

            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(dependsOnMethods = "testAKafkaPauseAndResume")
    public void testRecoveryOnFailureOfSingleNodeWithKafka() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        try {
            log.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
            String topics[] = new String[]{"kafka_topic4"};
            KafkaTestUtil.createTopic(topics, 1);
            PersistenceStore persistenceStore = new InMemoryPersistenceStore();
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setPersistenceStore(persistenceStore);
            String query = "@App:name('TestExecutionPlan') " +
                    "define stream BarStream (count long); " +
                    "@info(name = 'query1') " +
                    "@source(type='kafka', topic.list='kafka_topic4', group.id='test', " +
                    "threading.option='topic.wise', bootstrap.servers='localhost:9092', partition.no.list='0', " +
                    "@map(type='text'))" +
                    "Define stream FooStream (symbol string, price float, volume long);" +
                    "from FooStream select count(symbol) as count insert into BarStream;";
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        log.info(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });

            // start publishing events to Kafka
            Future eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    KafkaTestUtil.kafkaPublisher(topics, 1, 5, 1000, false, null, false);
                }
            });
            Thread.sleep(2000);
            // start the execution app
            siddhiAppRuntime.start();

            // wait for some time
            Thread.sleep(28000);
            // initiate a checkpointing task
            Future perisistor = siddhiAppRuntime.persist().getFuture();
            // waits till the checkpointing task is done
            while (!perisistor.isDone()) {
                Thread.sleep(100);
            }
            // let few more events to be published
            Thread.sleep(5000);
            // initiate a execution app shutdown - to demonstrate a node failure
            siddhiAppRuntime.shutdown();
            // let few events to be published while the execution app is down
            Thread.sleep(5000);
            // recreate the execution app
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        log.info(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });
            // start the execution app
            siddhiAppRuntime.start();
            // immediately trigger a restoreState from last revision
            siddhiAppRuntime.restoreLastRevision();
            Thread.sleep(5000);

            // waits till all the events are published
            while (!eventSender.isDone()) {
                Thread.sleep(2000);
            }

            Thread.sleep(20000);
            assertTrue(eventArrived);
            // assert the count
            assertEquals(5, count);

            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(dependsOnMethods = "testRecoveryOnFailureOfSingleNodeWithKafka")
    public void testRecoveryOnFailureOfMultipleNodeWithKafka() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        try {
            log.info("Test to verify recovering process of multiple Siddhi nodes on a failure when Kafka is the event"
                    + " source");
            String topics[] = new String[]{"kafka_topic5", "kafka_topic6"};
            KafkaTestUtil.createTopic(topics, 1);
            // 1st node
            PersistenceStore persistenceStore = new InMemoryPersistenceStore();
            SiddhiManager siddhiManager1 = new SiddhiManager();
            siddhiManager1.setPersistenceStore(persistenceStore);
//            siddhiManager1.setExtension("inputmapper:text", XmlSourceMapper.class);

            // 2nd node
            PersistenceStore persistenceStore1 = new InMemoryPersistenceStore();
            SiddhiManager siddhiManager2 = new SiddhiManager();
            siddhiManager2.setPersistenceStore(persistenceStore1);
//            siddhiManager2.setExtension("inputmapper:text", XmlSourceMapper.class);

            String query1 = "@App:name('TestExecutionPlan') " +
                    "@sink(type='kafka', topic='kafka_topic6', bootstrap.servers='localhost:9092', partition" +
                    ".no='0', " +
                    "@map(type='text'))" +
                    "define stream BarStream (count long); " +
                    "@source(type='kafka', topic.list='kafka_topic5', group.id='test', " +
                    "threading.option='topic.wise', bootstrap.servers='localhost:9092', partition.no.list='0', " +
                    "@map(type='text'))" +
                    "Define stream FooStream (symbol string, price float, volume long);" +
                    "@info(name = 'query1') " +
                    "from FooStream select count() as count insert into BarStream;";

            String query2 = "@App:name('TestExecutionPlan') " +
                    "define stream BarStream (count long); " +
                    "@source(type='kafka', topic.list='kafka_topic6', group.id='test', " +
                    "threading.option='topic.wise', bootstrap.servers='localhost:9092', partition.no.list='0', " +
                    "@map(type='text'))" +
                    "Define stream FooStream (count long);" +
                    "@info(name = 'query1') " +
                    "from FooStream select count() as count insert into BarStream;";

            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(query1);
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(query2);

            siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        log.info(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });

            // start the execution app
            siddhiAppRuntime1.start();
            siddhiAppRuntime2.start();
            // let it initialize
            Thread.sleep(2000);

            // start publishing events to Kafka
            Future eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    KafkaTestUtil.kafkaPublisher(
                            new String[]{"kafka_topic5"}, 1, 50, 1000, false, null, false);
                }
            });

            // wait for some time
            Thread.sleep(28000);
            // initiate a checkpointing task
            Future perisistor1 = siddhiAppRuntime1.persist().getFuture();
            Future perisistor2 = siddhiAppRuntime2.persist().getFuture();
            // waits till the checkpointing task is done
            while (!perisistor1.isDone() && !perisistor2.isDone()) {
                Thread.sleep(100);
            }
            // let few more events to be published
            Thread.sleep(5000);
            // initiate a execution app shutdown - to demonstrate a node failure
            siddhiAppRuntime1.shutdown();
            siddhiAppRuntime2.shutdown();
            // let few events to be published while the execution app is down
            Thread.sleep(5000);
            // recreate the execution app
            siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(query1);
            siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(query2);
            siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        log.info(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });
            // start the execution app
            siddhiAppRuntime1.start();
            siddhiAppRuntime2.start();
            // immediately trigger a restoreState from last revision
            siddhiAppRuntime1.restoreLastRevision();
            siddhiAppRuntime2.restoreLastRevision();
            Thread.sleep(5000);

            // waits till all the events are published
            while (!eventSender.isDone()) {
                Thread.sleep(2000);
            }

            Thread.sleep(20000);
            assertTrue(eventArrived);
            // assert the count
            assertEquals(50, count);

            siddhiAppRuntime1.shutdown();
            siddhiAppRuntime2.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test
    public void testCreatingFullKafkaEventFlow() throws InterruptedException {
        Runnable kafkaReceiver = new KafkaFlow();
        Thread t1 = new Thread(kafkaReceiver);
        t1.start();
        Thread.sleep(35000);
    }

    private class KafkaFlow implements Runnable {
        @Override
        public void run() {
            try {
                StreamDefinition inputDefinition = StreamDefinition.id("FooStream")
                        .attribute("symbol", Attribute.Type.STRING)
                        .attribute("price", Attribute.Type.FLOAT)
                        .attribute("volume", Attribute.Type.INT)
                        .annotation(Annotation.annotation("source")
                                .element("type", "kafka")
                                .element("topic", "receiver_topic")
                                .element("threads", "1")
                                .element("partition.no.list", "0,1")
                                .element("group.id", "group1")
                                .element("bootstrap.servers", "localhost:9092")
                                .annotation(Annotation.annotation("map")
                                        .element("type", "text")));

                StreamDefinition outputDefinition = StreamDefinition.id("BarStream")
                        .attribute("symbol", Attribute.Type.STRING)
                        .attribute("price", Attribute.Type.FLOAT)
                        .attribute("volume", Attribute.Type.INT)
                        .annotation(Annotation.annotation("sink")
                                .element("type", "kafka")
                                .element("topic", "publisher_topic")
                                .element("partition.no", "0")
                                .element("bootstrap.servers", "localhost:9092")
                                .annotation(Annotation.annotation("map")
                                        .element("type", "text")));

                Query query = Query.query();
                query.from(
                        InputStream.stream("FooStream")
                );
                query.select(
                        Selector.selector().select(new Variable("symbol")).select(new Variable("price")).select(new
                                Variable("volume"))
                );
                query.insertInto("BarStream");

                SiddhiManager siddhiManager = new SiddhiManager();
//                siddhiManager.setExtension("xml-input-mapper", XmlSourceMapper.class);
//                siddhiManager.setExtension("sink.mapper:text", XMLSinkMapper.class);

                SiddhiApp siddhiApp = new SiddhiApp("ep1");
                siddhiApp.defineStream(inputDefinition);
                siddhiApp.defineStream(outputDefinition);
                siddhiApp.addQuery(query);
                SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

                siddhiAppRuntime.addCallback("FooStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        log.info("Printing received events !!");
                        EventPrinter.print(events);
                    }
                });
                siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        log.info("Printing publishing events !!");
                        EventPrinter.print(events);
                    }
                });
                siddhiAppRuntime.start();
                Thread.sleep(30000);
                siddhiAppRuntime.shutdown();
            } catch (ZkTimeoutException ex) {
                log.warn("No zookeeper may not be available.", ex);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}

