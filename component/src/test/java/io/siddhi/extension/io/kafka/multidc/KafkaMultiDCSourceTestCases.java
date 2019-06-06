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

package io.siddhi.extension.io.kafka.multidc;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.extension.io.kafka.KafkaTestUtil;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class implementing the Test cases for Sequenced Messaging.
 */
public class KafkaMultiDCSourceTestCases {
    static final Logger LOG = Logger.getLogger(KafkaMultiDCSourceTestCases.class);
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
            KafkaTestUtil.cleanLogDir2();
            KafkaTestUtil.setupKafkaBroker2();
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RemoteException("Exception caught when starting server", e);
        }
    }

    @AfterClass
    public static void stopKafkaBroker() throws InterruptedException {
        KafkaTestUtil.stopKafkaBroker();
        Thread.sleep(1000);
        KafkaTestUtil.stopKafkaBroker2();
        Thread.sleep(1000);
        while (!executorService.isShutdown() || !executorService.isTerminated()) {
            executorService.shutdown();
        }
    }

    @BeforeMethod
    public void reset() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testMultiDCSourceWithBothBrokersRunning() throws InterruptedException {
        LOG.info("Creating test for publishing events for static topic without a partition");
        String topics[] = new String[]{"myTopic"};
        KafkaTestUtil.createTopic(KafkaTestUtil.ZK_SERVER_CON_STRING, topics, 1);
        KafkaTestUtil.createTopic(KafkaTestUtil.ZK_SERVER2_CON_STRING, topics, 1);
        Thread.sleep(4000);
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);

        SiddhiManager sourceOneSiddhiManager = new SiddhiManager();
        SiddhiAppRuntime sourceOneApp = sourceOneSiddhiManager.createSiddhiAppRuntime(
                "@App:name('SourceOneSiddhiApp') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='kafkaMultiDC', " +
                        "topic='myTopic', " +
                        "partition='0', " +
                        "bootstrap.servers='localhost:9092,localhost:9093'," +
                        "@map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");

        sourceOneApp.addCallback("BarStream2", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                for (Event event : events) {
                    LOG.info(event);
                    eventArrived = true;
                    count++;
                    receivedEventNameList.add(event.getData(0).toString());
                    receivedValueList.add((long) event.getData(2));
                }
            }
        });
        sourceOneApp.start();
        Thread.sleep(4000);


        String sinkApp = "@App:name('SinkSiddhiApp') \n"
                + "define stream FooStream (symbol string, price float, volume long); \n"
                + "@info(name = 'query1') \n"
                + "@sink("
                + "type='kafkaMultiDC', "
                + "topic='myTopic', "
                + "partition='0',"
                + "bootstrap.servers='localhost:9092,localhost:9093', "
                + "@map(type='xml'))" +
                "Define stream BarStream (symbol string, price float, volume long);\n" +
                "from FooStream select symbol, price, volume insert into BarStream;\n";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSink = siddhiManager.createSiddhiAppRuntime(sinkApp);
        InputHandler fooStream = siddhiAppRuntimeSink.getInputHandler("BarStream");
        siddhiAppRuntimeSink.start();
        Thread.sleep(4000);
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 75.6f, 102L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 103L});
        Thread.sleep(4000);

        Assert.assertTrue(count == 3);
    }

    @Test(description = "Test the scenario: MultiDC sink and source with binary mapper")
    public void testMultiDCSourceWithBothBrokersRunningUsingBinaryMapper() throws InterruptedException {
        LOG.info("Creating test for publishing events for static topic without a partition");
        String topics[] = new String[]{"myTopic2"};
        KafkaTestUtil.createTopic(KafkaTestUtil.ZK_SERVER_CON_STRING, topics, 1);
        KafkaTestUtil.createTopic(KafkaTestUtil.ZK_SERVER2_CON_STRING, topics, 1);
        Thread.sleep(4000);
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);

        SiddhiManager sourceOneSiddhiManager = new SiddhiManager();
        SiddhiAppRuntime sourceOneApp = sourceOneSiddhiManager.createSiddhiAppRuntime(
                "@App:name('SourceOneSiddhiApp') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='kafkaMultiDC', " +
                        "topic='myTopic2', " +
                        "partition='0', " +
                        "is.binary.message='true'," +
                        "bootstrap.servers='localhost:9092,localhost:9093'," +
                        "@map(type='binary'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");

        sourceOneApp.addCallback("BarStream2", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                for (Event event : events) {
                    LOG.info(event);
                    eventArrived = true;
                    count++;
                    receivedEventNameList.add(event.getData(0).toString());
                    receivedValueList.add((long) event.getData(2));
                }
            }
        });
        sourceOneApp.start();
        Thread.sleep(4000);


        String sinkApp = "@App:name('SinkSiddhiApp') \n"
                + "define stream FooStream (symbol string, price float, volume long); \n"
                + "@info(name = 'query1') \n"
                + "@sink("
                + "type='kafkaMultiDC', "
                + "topic='myTopic2', "
                + "is.binary.message='true',"
                + "partition='0',"
                + "bootstrap.servers='localhost:9092,localhost:9093', "
                + "@map(type='binary'))" +
                "Define stream BarStream (symbol string, price float, volume long);\n" +
                "from FooStream select symbol, price, volume insert into BarStream;\n";

        SiddhiManager siddhiManager = new SiddhiManager();

        SiddhiAppRuntime siddhiAppRuntimeSink = siddhiManager.createSiddhiAppRuntime(sinkApp);
        InputHandler fooStream = siddhiAppRuntimeSink.getInputHandler("BarStream");
        siddhiAppRuntimeSink.start();
        Thread.sleep(4000);
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 75.6f, 102L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 103L});
        Thread.sleep(4000);

        Assert.assertTrue(count == 3);
    }

    @Test(description = "Test the scenario: Send and Received event via multiDC sink and source as byte stream "
            + "using xml mapper")
    public void testMultiDCSourceWithBothBrokersRunningUsingXmlMapper() throws InterruptedException {
        LOG.info("Creating test for publishing events for static topic without a partition");
        String topics[] = new String[]{"myTopic3"};
        KafkaTestUtil.createTopic(KafkaTestUtil.ZK_SERVER_CON_STRING, topics, 1);
        KafkaTestUtil.createTopic(KafkaTestUtil.ZK_SERVER2_CON_STRING, topics, 1);
        Thread.sleep(4000);
        receivedEventNameList = new ArrayList<>(3);
        receivedValueList = new ArrayList<>(3);

        SiddhiManager sourceOneSiddhiManager = new SiddhiManager();
        SiddhiAppRuntime sourceOneApp = sourceOneSiddhiManager.createSiddhiAppRuntime(
                "@App:name('SourceOneSiddhiApp') " +
                        "define stream BarStream2 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@source(type='kafkaMultiDC', " +
                        "topic='myTopic3', " +
                        "partition='0', " +
                        "is.binary.message='true'," +
                        "bootstrap.servers='localhost:9092,localhost:9093'," +
                        "@map(type='xml'))" +
                        "Define stream FooStream2 (symbol string, price float, volume long);" +
                        "from FooStream2 select symbol, price, volume insert into BarStream2;");

        sourceOneApp.addCallback("BarStream2", new StreamCallback() {
            @Override
            public synchronized void receive(Event[] events) {
                for (Event event : events) {
                    LOG.info(event);
                    eventArrived = true;
                    count++;
                    receivedEventNameList.add(event.getData(0).toString());
                    receivedValueList.add((long) event.getData(2));
                }
            }
        });
        sourceOneApp.start();
        Thread.sleep(4000);


        String sinkApp = "@App:name('SinkSiddhiApp') \n"
                + "define stream FooStream (symbol string, price float, volume long); \n"
                + "@info(name = 'query1') \n"
                + "@sink("
                + "type='kafkaMultiDC', "
                + "topic='myTopic3', "
                + "is.binary.message='true',"
                + "partition='0',"
                + "bootstrap.servers='localhost:9092,localhost:9093', "
                + "@map(type='xml'))" +
                "Define stream BarStream (symbol string, price float, volume long);\n" +
                "from FooStream select symbol, price, volume insert into BarStream;\n";

        SiddhiManager siddhiManager = new SiddhiManager();

        SiddhiAppRuntime siddhiAppRuntimeSink = siddhiManager.createSiddhiAppRuntime(sinkApp);
        InputHandler fooStream = siddhiAppRuntimeSink.getInputHandler("BarStream");
        siddhiAppRuntimeSink.start();
        Thread.sleep(4000);
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 75.6f, 102L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 103L});
        Thread.sleep(4000);

        Assert.assertTrue(count == 3);
    }
}
