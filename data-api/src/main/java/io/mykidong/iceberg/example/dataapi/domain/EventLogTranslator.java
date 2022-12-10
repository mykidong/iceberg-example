package io.mykidong.iceberg.example.dataapi.domain;

import com.lmax.disruptor.EventTranslator;

public class EventLogTranslator extends EventLog implements EventTranslator<EventLog>{

    @Override
    public void translateTo(EventLog eventLog, long sequence) {
        eventLog.setUser(this.getUser());
        eventLog.setCatalog(this.getCatalog());
        eventLog.setSchema(this.getSchema());
        eventLog.setTable(this.getTable());
        eventLog.setJson(this.getJson());
    }
}
