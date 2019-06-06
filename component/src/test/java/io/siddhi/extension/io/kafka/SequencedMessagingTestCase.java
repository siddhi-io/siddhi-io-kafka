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

package io.siddhi.extension.io.kafka;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class implementing the Test cases for Sequenced Messaging.
 */
public class SequencedMessagingTestCase {
    static final Logger LOG = Logger.getLogger(SequencedMessagingTestCase.class);
    private static ExecutorService executorService;
    private volatile int count;
    private volatile boolean eventArrived;
    private volatile List<String> receivedEventNameList;
    private volatile List<Long> receivedValueList;

    @BeforeClass
    public static void init() throws Exception {
        try {
            executorService = Executors.newFixedThreadPool(5);
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
        while (!executorService.isShutdown() || !executorService.isTerminated()) {
            executorService.shutdown();
        }
        Thread.sleep(100);
    }

    @BeforeMethod
    public void reset() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void basicKafkaTestUsingBinaryMessage() throws InterruptedException, CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        String topics[] = new String[]{"ExternalTopic-0", "IntermediateTopic-0"};
        KafkaTestUtil.createTopic(topics, 1);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp1') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-0', bootstrap.servers='localhost:9092', " +
                "partition.no='0', sequence.id='ExternalDataRelayApp', is.binary.message='true', "
                + "@map(type='binary')) " +
                "define stream BarStream (symbol string, count long); " +

                "@source(type='kafka', topic.list='ExternalTopic-0', group.id='test1', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +

                "Define stream FooStream (symbol string, price float, volume long); " +

                "from FooStream select symbol, count() as count insert into BarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp2') " +
                "define stream BarStream1 (symbol string, count long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-0', group.id='test1', " +
                "threading.option='topic.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', is.binary.message='true', @map(type='binary')) " +
                "Define stream FooStream1 (symbol string, count long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                LOG.info(events);
                count += events.length;
            }
        });

        // Start the apps
        externalDataRelayApp.start();
        dataReceiveApp.start();

        // start publishing events to External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-0"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished publishing 5 events to the external topic.");

        // waits till the checkpointing task is done
        while (!externalDataRelayApp.persist().getFuture().isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit((Runnable) () -> KafkaTestUtil.kafkaPublisher(
                new String[]{"ExternalTopic-0"}, 1, 5, 100, false, null, true));
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        // Shutting down the external relay app to mimic a node failure and starting it again like a restart
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();

        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(500);

        Assert.assertEquals(count, 15);

        KafkaTestUtil.deleteTopic(topics);
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "basicKafkaTestUsingBinaryMessage")
    public void basicKafkaTestUsingBinaryMessageWithXmlMapper() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        String topics[] = new String[]{"ExternalTopic-xml", "IntermediateTopic-xml"};
        KafkaTestUtil.createTopic(topics, 1);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp1') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-xml', bootstrap.servers='localhost:9092', " +
                "partition.no='0', sequence.id='ExternalDataRelayApp', is.binary.message='true', "
                + "@map(type='xml')) " +
                "define stream BarStream (symbol string, count long); " +

                "@source(type='kafka', topic.list='ExternalTopic-xml', group.id='test1', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +

                "Define stream FooStream (symbol string, price float, volume long); " +

                "from FooStream select symbol, count() as count insert into BarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp2') " +
                "define stream BarStream1 (symbol string, count long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-xml', group.id='test1', " +
                "threading.option='topic.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', is.binary.message='true', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                LOG.info(events);
                count += events.length;
            }
        });

        // Start the apps
        externalDataRelayApp.start();
        dataReceiveApp.start();

        // start publishing events to External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-xml"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished publishing 5 events to the external topic.");

        // waits till the checkpointing task is done
        while (!externalDataRelayApp.persist().getFuture().isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit((Runnable) () -> KafkaTestUtil.kafkaPublisher(
                new String[]{"ExternalTopic-xml"}, 1, 5, 100, false, null, true));
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        // Shutting down the external relay app to mimic a node failure and starting it again like a restart
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();

        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(500);

        Assert.assertEquals(count, 15);

        KafkaTestUtil.deleteTopic(topics);
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "basicKafkaTestUsingBinaryMessageWithXmlMapper")
    public void basicTest() throws InterruptedException, CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        String topics[] = new String[]{"ExternalTopic-1", "IntermediateTopic-1"};
        KafkaTestUtil.createTopic(topics, 1);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp1') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-1', bootstrap.servers='localhost:9092', " +
                "partition.no='0', sequence.id='ExternalDataRelayApp', @map(type='xml')) " +
                "define stream BarStream (symbol string, count long); " +

                "@source(type='kafka', topic.list='ExternalTopic-1', group.id='test1', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream FooStream (symbol string, price float, volume long); " +

                "from FooStream select symbol, count() as count insert into BarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp2') " +
                "define stream BarStream1 (symbol string, count long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-1', group.id='test1', " +
                "threading.option='topic.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });

        // Start the apps
        externalDataRelayApp.start();
        dataReceiveApp.start();

        // start publishing events to External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-1"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished publishing 5 events to the external topic.");

        // waits till the checkpointing task is done
        while (!externalDataRelayApp.persist().getFuture().isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit((Runnable) () -> KafkaTestUtil.kafkaPublisher(
                new String[]{"ExternalTopic-1"}, 1, 5, 100, false, null, true));
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        // Shutting down the external relay app to mimic a node failure and starting it again like a restart
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();

        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(500);

        Assert.assertEquals(count, 15);

        KafkaTestUtil.deleteTopic(topics);
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "basicTest")
    public void testWithMultiplePartitionsSingleTopic() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        KafkaTestUtil.createTopic(new String[]{"ExternalTopic-2"}, 1);
        KafkaTestUtil.createTopic(new String[]{"IntermediateTopic-2"}, 3);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp2') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-2', bootstrap.servers='localhost:9092', " +
                "partition.no='{{count}}', sequence.id='ExternalDataRelayApp', @map(type='xml')) " +
                "Define stream BarStream (symbol string, count long, total long); " +

                "@source(type='kafka', topic.list='ExternalTopic-2', group.id='test2', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream FooStream (symbol string, price float, volume long); " +

                "from FooStream select symbol, count()%3 as count, count() as total insert into BarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp2') " +
                "Define stream BarStream1 (symbol string, count long, total long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-2', group.id='test2', " +
                "threading.option='topic.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0,1,2', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long, total long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });

        // start publishing events to External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-2"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished publishing 10 events to the external topic.");

        // Start the apps after events are published to the topic to let the events mixed up in the receiving order
        // for each partition
        LOG.info("Starting the Siddhi Apps");
        externalDataRelayApp.start();
        dataReceiveApp.start();

        Future perisistor = externalDataRelayApp.persist().getFuture();
        // waits till the checkpointing task is done
        while (!perisistor.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-2"}, 1, 5, 100, false, null, true);
            }
        });
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        // Shutting down the external relay app to mimic a node failure and starting it again like a restart so that
        // after the restart it will replay the last published 5 messages as it's not being persisted.
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();

        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(1000);

        Assert.assertEquals(count, 15);

        KafkaTestUtil.deleteTopic(new String[]{"ExternalTopic-2", "IntermediateTopic-2"});
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "testWithMultiplePartitionsSingleTopic")
    public void testWithMultiplePartitionsSingleTopicWithPartitionWiseThreading() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        KafkaTestUtil.createTopic(new String[]{"ExternalTopic-3"}, 1);
        KafkaTestUtil.createTopic(new String[]{"IntermediateTopic-3"}, 3);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp3') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-3', bootstrap.servers='localhost:9092', " +
                "partition.no='{{count}}', sequence.id='ExternalDataRelayApp', @map(type='xml')) " +
                "Define stream BarStream (symbol string, count long, total long); " +

                "@source(type='kafka', topic.list='ExternalTopic-3', group.id='test3', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream FooStream (symbol string, price float, volume long); " +

                "from FooStream select symbol, count()%3 as count, count() as total insert into BarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp3') " +
                "Define stream BarStream1 (symbol string, count long, total long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-3', group.id='test3', " +
                "threading.option='partition.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0,1,2', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long, total long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });

        // start publishing events to External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-3"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished publishing 10 events to the external topic.");

        // Start the apps after events are published to the topic to let the events mixed up in the receiving order
        // for each partition
        LOG.info("Starting the Siddhi Apps");
        externalDataRelayApp.start();
        dataReceiveApp.start();

        Future perisistor = externalDataRelayApp.persist().getFuture();
        // waits till the checkpointing task is done
        while (!perisistor.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-3"}, 1, 5, 100, false, null, true);
            }
        });
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        // Shutting down the external relay app to mimic a node failure and starting it again like a restart so that
        // after the restart it will replay the last published 5 messages as it's not being persisted.
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();

        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(1000);

        Assert.assertEquals(count, 15);

        KafkaTestUtil.deleteTopic(new String[]{"ExternalTopic-3", "IntermediateTopic-3"});
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "testWithMultiplePartitionsSingleTopicWithPartitionWiseThreading")
    public void testWithMultipleSeqIdsAndMultiplePartitionsWithTopicWiseThreading() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        KafkaTestUtil.createTopic(new String[]{"ExternalTopic-4"}, 1);
        KafkaTestUtil.createTopic(new String[]{"SecondExternalTopic-4"}, 1);
        KafkaTestUtil.createTopic(new String[]{"IntermediateTopic-4"}, 3);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        //Receive data from two external topics. And two different Sinks publishes events to the same intermediate
        // topic which is consumed by another siddhi app using two different sequence ids.
        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp4') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-4', bootstrap.servers='localhost:9092', " +
                "partition.no='{{count}}', sequence.id='ExternalDataRelayAppSink1', @map(type='xml')) " +
                "Define stream BarStream (symbol string, count long, total long); " +

                "@sink(type='kafka', topic='IntermediateTopic-4', bootstrap.servers='localhost:9092', " +
                "partition.no='{{count}}', sequence.id='ExternalDataRelayAppSink2', @map(type='xml')) " +
                "Define stream SecondBarStream (symbol string, count long, total long); " +

                "@source(type='kafka', topic.list='ExternalTopic-4', group.id='test4', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream FooStream (symbol string, price float, volume long); " +

                "@source(type='kafka', topic.list='SecondExternalTopic-4', group.id='test4', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream SecondFooStream (symbol string, price float, volume long); " +

                "From FooStream select symbol, count()%3 as count, count() as total insert into BarStream;" +

                "From SecondFooStream select symbol, count()%3 as count, count() as total insert into SecondBarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp4') " +
                "Define stream BarStream1 (symbol string, count long, total long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-4', group.id='test4', " +
                "threading.option='partition.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0,1,2', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long, total long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });

        // start publishing events to the first External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-4", "SecondExternalTopic-4"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        LOG.info("Finished publishing 10 events to the external topic.");

        // Start the apps after events are published to the topic to let the events mixed up in the receiving order
        // for each partition
        LOG.info("Starting the Siddhi Apps");
        externalDataRelayApp.start();
        dataReceiveApp.start();

        Future perisistor = externalDataRelayApp.persist().getFuture();
        // waits till the checkpointing task is done
        while (!perisistor.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-4", "SecondExternalTopic-4"}, 1, 5, 100, false, null, true);
            }
        });
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }


        // Shutting down the external relay app to mimic a node failure and starting it again like a restart so that
        // after the restart it will replay the last published 5 messages as it's not being persisted.
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();
        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(2000);

        Assert.assertEquals(count, 30);

        KafkaTestUtil.deleteTopic(new String[]{"ExternalTopic-4", "SecondExternalTopic-4", "IntermediateTopic-4"});
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "testWithMultipleSeqIdsAndMultiplePartitionsWithTopicWiseThreading")
    public void testWithMultipleSeqIdsSinglePartitionWithTopicWiseThreading() throws InterruptedException,
            CannotRestoreSiddhiAppStateException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        KafkaTestUtil.createTopic(new String[]{"ExternalTopic-5"}, 1);
        KafkaTestUtil.createTopic(new String[]{"SecondExternalTopic-5"}, 1);
        KafkaTestUtil.createTopic(new String[]{"IntermediateTopic-5"}, 1);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        //Receive data from two external topics. And two different Sinks publishes events to the same intermediate
        // topic which is consumed by another siddhi app using two different sequence ids.
        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp5') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-5', bootstrap.servers='localhost:9092', " +
                "partition.no='0', sequence.id='ExternalDataRelayAppSink1', @map(type='xml')) " +
                "Define stream BarStream (symbol string, count long); " +

                "@sink(type='kafka', topic='IntermediateTopic-5', bootstrap.servers='localhost:9092', " +
                "partition.no='0', sequence.id='ExternalDataRelayAppSink2', @map(type='xml')) " +
                "Define stream SecondBarStream (symbol string, count long); " +

                "@source(type='kafka', topic.list='ExternalTopic-5', group.id='test5', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream FooStream (symbol string, price float, volume long); " +

                "@source(type='kafka', topic.list='SecondExternalTopic-5', group.id='test5', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +
                "Define stream SecondFooStream (symbol string, price float, volume long); " +

                "From FooStream select symbol, count() as count insert into BarStream;" +

                "From SecondFooStream select symbol, count() as count insert into SecondBarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp5') " +
                "Define stream BarStream1 (symbol string, count long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-5', group.id='test5', " +
                "threading.option='topic.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });

        // start publishing events to the first External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-5", "SecondExternalTopic-5"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }

        LOG.info("Finished publishing 10 events to the external topic.");

        // Start the apps after events are published to the topic to let the events mixed up in the receiving order
        // for each partition
        LOG.info("Starting the Siddhi Apps");
        externalDataRelayApp.start();
        dataReceiveApp.start();

        Future perisistor = externalDataRelayApp.persist().getFuture();
        // waits till the check pointing task is done
        while (!perisistor.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished persisting the state of external event relay siddhi app");

        // Send more events after persisting the state
        eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-5", "SecondExternalTopic-5"}, 1, 5, 100, false, null, true);
            }
        });
        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }


        // Shutting down the external relay app to mimic a node failure and starting it again like a restart so that
        // after the restart it will replay the last published 5 messages as it's not being persisted.
        LOG.info("Restarting the external relay Siddhi App to mimic a node failure and a restart");
        externalDataRelayApp.shutdown();
        externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        externalDataRelayApp.start();

        // Restore the state from last snapshot that was taken before shutdown
        externalDataRelayApp.restoreLastRevision();
        Thread.sleep(2000);

        Assert.assertEquals(count, 30);

        KafkaTestUtil.deleteTopic(new String[]{"ExternalTopic-5", "SecondExternalTopic-5", "IntermediateTopic-5"});
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

    @Test(dependsOnMethods = "testWithMultipleSeqIdsSinglePartitionWithTopicWiseThreading")
    public void testWhenSequenceNumberIsNotAdded() throws InterruptedException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        String topics[] = new String[]{"ExternalTopic-6"};
        KafkaTestUtil.createTopic(topics, 1);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp6') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-6', bootstrap.servers='localhost:9092', " +
                "partition.no='0', @map(type='xml')) " +
                "define stream BarStream (symbol string, count long); " +

                "@source(type='kafka', topic.list='ExternalTopic-6', group.id='test6', " +
                "threading.option='topic.wise', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml'))" +

                "Define stream FooStream (symbol string, price float, volume long); " +

                "from FooStream select symbol, count() as count insert into BarStream;";

        final String dataReceiveQuery = "@App:name('DataReceiveApp6') " +
                "define stream BarStream1 (symbol string, count long); " +

                "@info(name = 'DataReceiveQuery') " +
                "@source(type='kafka', topic.list='IntermediateTopic-6', group.id='test6', " +
                "threading.option='topic.wise', seq.enabled='true', bootstrap.servers='localhost:9092', " +
                "partition.no.list='0', @map(type='xml')) " +
                "Define stream FooStream1 (symbol string, count long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });

        // Start the apps
        externalDataRelayApp.start();
        dataReceiveApp.start();

        // start publishing events to External Kafka topic
        Future eventSender = executorService.submit(new Runnable() {
            @Override
            public void run() {
                KafkaTestUtil.kafkaPublisher(
                        new String[]{"ExternalTopic-6"}, 1, 10, 100, false, null, true);
            }
        });

        while (!eventSender.isDone()) {
            Thread.sleep(100);
        }
        LOG.info("Finished publishing 10 events to the external topic.");

        Assert.assertEquals(count, 0);

        KafkaTestUtil.deleteTopic(topics);
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
        Thread.sleep(2000);
    }

}
