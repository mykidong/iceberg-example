package io.mykidong.iceberg.example.dataapi.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.arteam.embedhttp.EmbeddedHttpServer;
import com.github.arteam.embedhttp.HttpHandler;
import com.github.arteam.embedhttp.HttpRequest;
import com.github.arteam.embedhttp.HttpResponse;
import io.mykidong.iceberg.example.dataapi.domain.EventLog;
import io.mykidong.iceberg.example.dataapi.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DisruptorHttpServer {

    private static Logger LOG = LoggerFactory.getLogger(DisruptorHttpServer.class);

    public static final int PORT = 12345;

    private ConcurrentLinkedQueue<EventLog> eventLogQueue;
    private EmbeddedHttpServer embeddedHttpServer;

    public DisruptorHttpServer(ConcurrentLinkedQueue<EventLog> eventLogQueue) {
        this.eventLogQueue = eventLogQueue;

        embeddedHttpServer = new EmbeddedHttpServer();
        embeddedHttpServer.addHandler("/event_log", new GetEventLog(this.eventLogQueue));
        embeddedHttpServer.start(PORT);
        LOG.info("simple http server started...");
    }

    private static class GetEventLog implements HttpHandler {
        private ObjectMapper mapper = new ObjectMapper();
        private ConcurrentLinkedQueue<EventLog> eventLogQueue;

        public GetEventLog(ConcurrentLinkedQueue<EventLog> eventLogQueue) {
            this.eventLogQueue = eventLogQueue;
        }

        @Override
        public void handle(HttpRequest request, HttpResponse response) throws IOException {
            EventLog eventLog = this.eventLogQueue.poll();
            String content = "";
            if (eventLog != null) {
                content = JsonUtils.toJson(mapper, eventLog);
            }
            response.setBody(content).addHeader("content-type", "text/plain");
        }
    }
}
