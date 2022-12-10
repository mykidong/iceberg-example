package io.mykidong.iceberg.example.dataapi.component;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.EventHandler;
import io.mykidong.iceberg.example.dataapi.domain.EventLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class DisruptorConsumer implements EventHandler<EventLog> {

    private static Logger LOG = LoggerFactory.getLogger(DisruptorConsumer.class);

    private ObjectMapper mapper = new ObjectMapper();

    private ConcurrentLinkedQueue<EventLog> eventLogQueue;

    public DisruptorConsumer(ConcurrentLinkedQueue<EventLog> eventLogQueue) {
        this.eventLogQueue = eventLogQueue;
    }

    @Override
    public void onEvent(EventLog eventLog, long sequence, boolean endOfBatch) {
        eventLogQueue.offer(eventLog);
    }
}
