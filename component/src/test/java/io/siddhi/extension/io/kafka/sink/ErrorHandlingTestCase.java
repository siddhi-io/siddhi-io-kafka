/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ErrorHandlingTestCase {
    static final Logger LOG = Logger.getLogger(ErrorHandlingTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void initClassVariables() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testErrorStream() throws InterruptedException {
        LOG.info("Sending messages to error stream when the broker is not available");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntimeSource = siddhiManager.createSiddhiAppRuntime(
                "@App:name('ErrorHandlerApp') " +
                        "define stream inputStream (symbol string, price float, volume long); " +
                        "define stream kafkaErrorStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@OnError(action='STREAM')" +
                        "@sink(type='kafka', topic='single_topic', is.synchronous='true', " +
                        "on.error='STREAM', " +
                        "bootstrap.servers='localhost:9092',\n" +
                        "optional.configuration='retry.backoff.ms:1,metadata.fetch.timeout.ms:10," +
                        "request.timeout.ms:5," +
                        "timeout.ms:10', " +
                        "@map(type='xml'))" +
                        "Define stream kafkaSinkStream (symbol string, price float, volume long);" +
                        "from inputStream select symbol, price, volume insert into kafkaSinkStream;" +
                        "from !kafkaSinkStream select symbol, price, volume insert into kafkaErrorStream;");
        siddhiAppRuntimeSource.addCallback("kafkaErrorStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    LOG.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });
        siddhiAppRuntimeSource.start();
        InputHandler fooStream = siddhiAppRuntimeSource.getInputHandler("inputStream");
        fooStream.send(new Object[]{"single_topic", 55.6f, 100L});
        Thread.sleep(1000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(1, count);
        siddhiAppRuntimeSource.shutdown();
    }
}
