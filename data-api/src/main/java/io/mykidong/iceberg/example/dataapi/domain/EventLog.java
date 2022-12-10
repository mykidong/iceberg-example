package io.mykidong.iceberg.example.dataapi.domain;

import com.lmax.disruptor.EventFactory;

import java.io.Serializable;

public class EventLog implements Serializable {

    public final static EventFactory<EventLog> FACTORY = EventLog::new;

    private String user;
    private String catalog;
    private String schema;
    private String table;
    private String json;

    public EventLog() {}

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }
}
