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
package io.siddhi.extension.io.kafka.multidc.source;

import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * The source Synchronize to merge events from two kafka source
 */
public class SourceSynchronizer {
    private static final Logger LOG = Logger.getLogger(SourceSynchronizer.class);
    private final SourceEventListener eventListener;
    boolean isEventGap = false;
    // Buffer events sorting by the sequence number.
    Map<Long, BufferValueHolder> eventBuffer = new TreeMap<>();
    Map<String, Long> perSourceReceivedSeqNo = new HashMap<>();
    Timer flushBufferTimer = new Timer(true);
    String[] bootstrapServers = new String[2];
    List<Long> toRemoveSeqNos = new ArrayList<>();
    private Long lastConsumedSeqNo = -1L;
    private int maxBufferSize;
    private int bufferInterval;
    private AtomicBoolean isFlushTaskDue = new AtomicBoolean(false);

    public SourceSynchronizer(SourceEventListener eventListener, String[] bootstrapServers, int maxBufferSize,
                              int bufferFlushInterval) {
        this.eventListener = eventListener;
        this.bootstrapServers[0] = bootstrapServers[0];
        this.bootstrapServers[1] = bootstrapServers[1];
        this.maxBufferSize = maxBufferSize;
        this.bufferInterval = bufferFlushInterval;

        perSourceReceivedSeqNo.put(bootstrapServers[0], -1L);
        perSourceReceivedSeqNo.put(bootstrapServers[1], -1L);
    }

    private synchronized void forceFlushBuffer(long flushTillSeqNo) {
        for (Map.Entry<Long, BufferValueHolder> entry : eventBuffer.entrySet()) {
            Long sequenceNumber = entry.getKey();
            BufferValueHolder eventHolder = entry.getValue();
            if ((sequenceNumber > lastConsumedSeqNo) &&
                    (sequenceNumber <= flushTillSeqNo)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Updating the lastConsumedSeqNo=" + sequenceNumber
                            + " as the event is forcefully flushed,"
                            + " from the source " + eventHolder.getSourceId());
                }

                if (!(sequenceNumber < lastConsumedSeqNo) &&
                        (lastConsumedSeqNo != sequenceNumber + 1)) {
                    LOG.warn("Events lost from sequence " + (lastConsumedSeqNo + 1)
                            + " to " + (sequenceNumber - 1));
                }

                lastConsumedSeqNo = sequenceNumber;
                toRemoveSeqNos.add(sequenceNumber);
                eventListener.onEvent(eventHolder.getEvent(), eventHolder.getStrings());
            }
        }
        toRemoveSeqNos.forEach(seqNo -> eventBuffer.remove(seqNo)); // To avoid concurrent modification.
        toRemoveSeqNos.clear();
    }

    private synchronized void flushBuffer() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start flushing buffer");
        }
        for (Map.Entry<Long, BufferValueHolder> entry : eventBuffer.entrySet()) {
            Long sequenceNumber = entry.getKey();
            BufferValueHolder eventHolder = entry.getValue();
            if (sequenceNumber <= lastConsumedSeqNo) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Message with sequence " + sequenceNumber + " already received. " +
                            "Dropping the event from the buffer");
                }
                toRemoveSeqNos.add(sequenceNumber);
                continue;
            } else if (sequenceNumber == lastConsumedSeqNo + 1) {
                isEventGap = false;
                lastConsumedSeqNo++;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Message with sequence " + sequenceNumber
                            + " flushed from buffer. Updating lastConsumedSeqNo=" + lastConsumedSeqNo);
                }

                toRemoveSeqNos.add(sequenceNumber);
                eventListener.onEvent(eventHolder.getEvent(), eventHolder.getStrings());
            } else {
                isEventGap = true;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Gap detected while flushing the buffer. Flushed message sequence=" + sequenceNumber
                            + ". Expected sequence=" + (lastConsumedSeqNo + 1)
                            + ". Stop flushing the buffer.");
                }
                break;
            }
        }

        toRemoveSeqNos.forEach(seqNo -> eventBuffer.remove(seqNo)); // To avoid concurrent modification.
        toRemoveSeqNos.clear();
        if (LOG.isDebugEnabled()) {
            LOG.debug("End flushing buffer");
        }
    }

    private synchronized void bufferEvent(String sourceId, long sequenceNumber, Object event, String[] strings) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Buffering Event. SourceId=" + sourceId + ", SequenceNumber=" + sequenceNumber);
        }

        if (eventBuffer.size() >= maxBufferSize) {
            long flushTillSeq = Math.max(
                    perSourceReceivedSeqNo.get(bootstrapServers[0]),
                    perSourceReceivedSeqNo.get(bootstrapServers[1]));
            LOG.info("Buffer size exceeded. Force flushing events till the sequence " + sequenceNumber);
            forceFlushBuffer(flushTillSeq);
        }
        eventBuffer.put(sequenceNumber, new BufferValueHolder(event, sourceId, strings));
    }

    public synchronized void onEvent(String sourceId, long sequenceNumber, Object event, String[] strings) {
        perSourceReceivedSeqNo.put(sourceId, sequenceNumber);

        if (sequenceNumber <= lastConsumedSeqNo) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Message with sequence " + sequenceNumber + " already received. " +
                        "Dropping the event from source " + sourceId + ":" + event);
            }
        } else if (sequenceNumber == lastConsumedSeqNo + 1) {
            lastConsumedSeqNo++;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Message with sequence " + sequenceNumber
                        + " received from source "
                        + sourceId + ". Updating lastConsumedSeqNo=" + lastConsumedSeqNo);
            }
            eventListener.onEvent(event, strings);

            // Gap is filled by receiving the next expected sequence number
            if (!eventBuffer.isEmpty()) {
                flushBuffer();
            }
        } else { // Sequence number is greater than the expected sequence number
            if (isEventGap) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Message with sequence " + sequenceNumber + " from source" + sourceId
                            + ". Couldn't fill the gap, buffering the event.");
                }

                bufferEvent(sourceId, sequenceNumber, event, strings);
                long flushTillSeq = Math.min(perSourceReceivedSeqNo.get(bootstrapServers[0]),
                        perSourceReceivedSeqNo.get(bootstrapServers[1]));
                isEventGap = false;
                forceFlushBuffer(flushTillSeq);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Gap detected. Message with sequence " + sequenceNumber
                            + " received from source " + sourceId
                            + ". Expected sequence number is " + (lastConsumedSeqNo + 1)
                            + ". Starting buffering events");
                }
                isEventGap = true;
                bufferEvent(sourceId, sequenceNumber, event, strings);

                if (!isFlushTaskDue.get()) {
                    flushBufferTimer.schedule(new BufferFlushTask(), bufferInterval);
                    isFlushTaskDue.set(true);
                }
            }
        }
    }

    public synchronized Long getLastConsumedSeqNo() {
        return lastConsumedSeqNo;
    }

    public synchronized void setLastConsumedSeqNo(long seqNo) {
        this.lastConsumedSeqNo = seqNo;
    }

    static class BufferValueHolder {
        String[] strings;
        private Object event;
        private String sourceId;

        BufferValueHolder(Object event, String sourceId, String[] strings) {
            this.event = event;
            this.sourceId = sourceId;
            this.strings = strings;
        }

        String[] getStrings() {
            return strings;
        }

        String getSourceId() {
            return sourceId;
        }

        public Object getEvent() {
            return event;
        }
    }

    class BufferFlushTask extends TimerTask {
        private final Logger log = Logger.getLogger(BufferFlushTask.class);

        @Override
        public synchronized void run() {
            isFlushTaskDue.set(false);
            long flushTillSeq = Math.max(perSourceReceivedSeqNo.get(bootstrapServers[0]),
                    perSourceReceivedSeqNo.get(bootstrapServers[1]));
            if (log.isDebugEnabled()) {
                log.debug("Executing the buffer flushing task. Flushing buffers till " + flushTillSeq);
            }
            forceFlushBuffer(flushTillSeq);
        }
    }
}
