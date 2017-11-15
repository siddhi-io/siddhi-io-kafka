package org.wso2.extension.siddhi.io.kafka;

import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class implementing to test cases for kafka using binary mapper.
 */
public class KafkaWithBinaryMapperTestCase {
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
    public static void stopKafkaBroker() {
        KafkaTestUtil.stopKafkaBroker();
    }

    @BeforeMethod
    public void reset() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void basicKafkaTestUsingBinaryMessage() throws InterruptedException {
        LOG.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
        String topics[] = new String[]{"ExternalTopic-1", "IntermediateTopic-1"};
        KafkaTestUtil.createTopic(topics, 1);
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        final String externalDataRelayQuery = "@App:name('ExternalDataRelayApp1') " +
                "@info(name = 'ExternalEventRelayQuery') " +
                "@sink(type='kafka', topic='IntermediateTopic-1', bootstrap.servers='localhost:9092', " +
                "partition.no='0', sequence.id='ExternalDataRelayApp', is.binary.message='true', "
                + "@map(type='binary')) " +
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
                "partition.no.list='0', is.binary.message='true', @map(type='binary')) " +
                "Define stream FooStream1 (symbol string, count long);" +

                "from FooStream1 select * insert into BarStream1;";

        SiddhiAppRuntime externalDataRelayApp = siddhiManager.createSiddhiAppRuntime(externalDataRelayQuery);
        SiddhiAppRuntime dataReceiveApp = siddhiManager.createSiddhiAppRuntime(dataReceiveQuery);

        dataReceiveApp.addCallback("BarStream1", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                count += events.length;
            }
        });

        // Start the apps
        externalDataRelayApp.start();
        dataReceiveApp.start();

        Thread.sleep(2000);

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
                new String[]{"ExternalTopic-1"}, 1, 5, 400, false, null, true));
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
        Thread.sleep(4000);

        Assert.assertEquals(count, 15);

        KafkaTestUtil.deleteTopic(topics);
        Thread.sleep(4000);
        externalDataRelayApp.shutdown();
        dataReceiveApp.shutdown();
    }

    @Test
    public void testPublisherUsingBinaryMapper() throws InterruptedException {
        LOG.info("Creating test for publishing events using binary mapper.");
        String topics[] = new String[]{"single_topic"};
        KafkaTestUtil.createTopic(topics, 1);
        Thread.sleep(4000);
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
            Thread.sleep(4000);
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
            Thread.sleep(4000);
            fooStream.send(new Object[]{"single_topic", 55.6f, 100L});
            fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
            fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
            Thread.sleep(4000);
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
            Thread.sleep(4000);
            siddhiAppRuntime.shutdown();
            siddhiAppRuntimeSource.shutdown();
        } catch (ZkTimeoutException ex) {
            LOG.warn("No zookeeper may not be available.", ex);
        }
    }

}
