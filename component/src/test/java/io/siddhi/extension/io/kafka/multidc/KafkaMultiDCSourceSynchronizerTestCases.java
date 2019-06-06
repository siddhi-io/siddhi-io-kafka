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

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.kafka.multidc.source.SourceSynchronizer;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.testng.annotations.BeforeMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Class implementing the Test cases for KafkaMultiDCSource Synchronize Test Case.
 */
public class KafkaMultiDCSourceSynchronizerTestCases {
    private static final Logger LOG = Logger.getLogger(KafkaMultiDCSourceSynchronizerTestCases.class);
    private static final String SOURCE_1 = "source:9000";
    private static final String SOURCE_2 = "source2:9000";
    private static String[] servers = {SOURCE_1, SOURCE_2};
    private List<Object> eventsArrived = new ArrayList<>();
    private SourceEventListener eventListener = new SourceEventListener() {
        @Override
        public StreamDefinition getStreamDefinition() {
            return null;
        }

        @Override
        public void onEvent(Object o, String[] strings) {
            eventsArrived.add(o);
        }

        @Override
        public void onEvent(Object o, String[] strings, String[] strings1) {
            eventsArrived.add(o);
        }
    };

    private static String buildDummyEvent(String source, long seqNo) {
        StringBuilder builder = new StringBuilder();
        builder.append(source).append(":").append(seqNo);
        return builder.toString();
    }

    @BeforeMethod
    public void reset() {
        eventsArrived.clear();
    }

    private boolean compareEventSequnce(List<Object> expectedEventSequence) {
        if (eventsArrived.size() != expectedEventSequence.size()) {
            LOG.info("Expected number of events and actual number of events are different. " +
                    "Expected=" + expectedEventSequence.size() + ". Arrived=" + eventsArrived.size());
            return false;
        }
        for (int i = 0; i < expectedEventSequence.size(); i++) {
            if (!eventsArrived.get(i).toString().equals(expectedEventSequence.get(i))) {
                LOG.warn("Event " + i + " in the expected and arrived events are different."
                        + " Expected=" + expectedEventSequence.get(i).toString()
                        + ", Arrived=" + eventsArrived.get(i).toString());
                return false;
            }
        }
        return true;
    }

    private void sendEvent(SourceSynchronizer synchronizer, String source, long seqNo) {
        String[] dummyDynamicOptions = new String[2];
        synchronizer.onEvent(source, seqNo, buildDummyEvent(source, seqNo), dummyDynamicOptions);
    }

    /*
        Source1: 0 - 1 - 2
        Source2:   0 - 1 - 2
     */
    @Test
    public void testWithoutGaps1() {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 1000);

        sendEvent(synchronizer, SOURCE_1, 0);

        sendEvent(synchronizer, SOURCE_2, 0);

        sendEvent(synchronizer, SOURCE_1, 1);

        sendEvent(synchronizer, SOURCE_2, 1);

        sendEvent(synchronizer, SOURCE_1, 2);

        sendEvent(synchronizer, SOURCE_2, 2);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 2));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 1 2
    Source2:       0 1 2
    */
    @Test
    public void testWithoutGaps2() {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 1);
        sendEvent(synchronizer, SOURCE_1, 2);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 2);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 2));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 - - - 1 2
    Source2:   0 1 2
    */
    @Test
    public void testWithoutGaps3() {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 1000);

        sendEvent(synchronizer, SOURCE_1, 0);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 2);

        sendEvent(synchronizer, SOURCE_1, 1);
        sendEvent(synchronizer, SOURCE_1, 2);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 2));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 <gap> 2 - - -
    Source2:           0 1 2
    */
    @Test
    public void testGapFiledByOtherSource() {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 2);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 2);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 2));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 <gap> 4 - - - - 5
    Source2:           0 1 2 3
    */
    @Test
    public void testMultiMessageGapFiledByOtherSource() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 2);
        sendEvent(synchronizer, SOURCE_2, 3);

        sendEvent(synchronizer, SOURCE_1, 5);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 2));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 3));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 5));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
        Source1: 0 <gap> 4 - - 5
        Source2:           0 1 - 2 3
    */
    @Test
    public void testMultiMessageGapFiledByOtherSource1() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);

        sendEvent(synchronizer, SOURCE_1, 5);

        sendEvent(synchronizer, SOURCE_2, 2);
        sendEvent(synchronizer, SOURCE_2, 3);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 2));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 3));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 5));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1:  <gap> 3 - - 4
    Source2:           0 1 - 2 3
    */
    @Test
    public void testMultiMessageGapFiledByOtherSource2() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 3);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);

        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 2);
        sendEvent(synchronizer, SOURCE_2, 3);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_2, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 2));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 3));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }


    /*
    Source1:  <gap> 3 4
    Source2:            0 1 2 <gap> 5
    */
    @Test
    public void testMultiMessageGapFiledByOtherSource3() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 3);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 2);
        sendEvent(synchronizer, SOURCE_2, 5);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_2, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 2));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 3));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 5));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 1 <gap> 4 - 5
    Source2: 0 1 - - -   6
    */
    @Test
    public void testUnrecoverableGap() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 1);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 6);

        sendEvent(synchronizer, SOURCE_1, 5);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 5));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 6));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 1 <gap> 4 - 5 <gap> 8 9
    Source2: 0 1 <gap> - 8   <gap>
    */
    @Test
    public void testTwoUnrecoverableGaps() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 1);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 8);

        sendEvent(synchronizer, SOURCE_1, 5);
        sendEvent(synchronizer, SOURCE_1, 8);
        sendEvent(synchronizer, SOURCE_1, 9);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 5));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 8));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 9));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 1 <gap> 4 - 5 <gap> 8 9  -  -  12
    Source2: 0 1 <gap> - 8   <gap> - -  10 11
    */
    @Test
    public void testMultipleoUnrecoverableGaps() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 1);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 8);

        sendEvent(synchronizer, SOURCE_1, 5);
        sendEvent(synchronizer, SOURCE_1, 8);
        sendEvent(synchronizer, SOURCE_1, 9);

        sendEvent(synchronizer, SOURCE_2, 10);
        sendEvent(synchronizer, SOURCE_2, 11);

        sendEvent(synchronizer, SOURCE_1, 12);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 5));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 8));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 9));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 10));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 11));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 12));


        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

    /*
    Source1: 0 1 <gap> 4 - 5 <gap> 8 9
    Source2: 0 1 <gap> - 8   <gap>     11 12
    */
    @Test
    public void testTwoUnrecoverableGapFlushedWithTimer() throws InterruptedException {
        SourceSynchronizer synchronizer = new SourceSynchronizer(eventListener, servers, 1000, 10 * 1000);

        sendEvent(synchronizer, SOURCE_1, 0);
        sendEvent(synchronizer, SOURCE_1, 1);
        sendEvent(synchronizer, SOURCE_1, 4);

        sendEvent(synchronizer, SOURCE_2, 0);
        sendEvent(synchronizer, SOURCE_2, 1);
        sendEvent(synchronizer, SOURCE_2, 8);

        sendEvent(synchronizer, SOURCE_1, 5);
        sendEvent(synchronizer, SOURCE_1, 8);
        sendEvent(synchronizer, SOURCE_1, 9);

        sendEvent(synchronizer, SOURCE_2, 11);
        sendEvent(synchronizer, SOURCE_2, 12);

        // Since no events are received from source1, it will wait for a tolerance period expecting the events to come.
        // When the time expires it will forcefully flush the events.
        Thread.sleep(15 * 1000);

        List<Object> expectedEvents = new ArrayList<>();
        expectedEvents.add(buildDummyEvent(SOURCE_1, 0));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 1));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 4));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 5));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 8));
        expectedEvents.add(buildDummyEvent(SOURCE_1, 9));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 11));
        expectedEvents.add(buildDummyEvent(SOURCE_2, 12));

        Assert.assertTrue(compareEventSequnce(expectedEvents));
    }

}
