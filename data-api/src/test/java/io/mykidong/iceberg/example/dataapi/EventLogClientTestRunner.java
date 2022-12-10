package io.mykidong.iceberg.example.dataapi;

import io.mykidong.iceberg.example.dataapi.component.SimpleHttpClient;
import io.mykidong.iceberg.example.dataapi.domain.ResponseHandler;
import io.mykidong.iceberg.example.dataapi.domain.RestResponse;
import io.mykidong.iceberg.example.dataapi.util.JsonUtils;
import io.mykidong.iceberg.example.dataapi.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.junit.Test;

import java.util.Map;

public class EventLogClientTestRunner {

    private SimpleHttpClient simpleHttpClient = new SimpleHttpClient();


    @Test
    public void pushEventLogs() throws Exception {
        String host = System.getProperty("host", "http://localhost:8097");
        String eventCount = System.getProperty("eventCount", "1000");

        String schema = "iceberg_db";
        String table = "test_iceberg";
        String json = StringUtils.fileToString("data/test.json", true);
        String lines[] = json.split("\\r?\\n");


        String urlPath = host + "/v1/event_log/create";

        int MAX = Integer.valueOf(eventCount);
        int count = 0;
        while(true) {
            for (String jsonLine : lines) {
                FormBody.Builder builder = new FormBody.Builder();
                builder.add("schema", schema);
                builder.add("table", table);
                builder.add("json", jsonLine);

                // parameters in body.
                RequestBody body = builder.build();
                Request request = new Request.Builder()
                        .url(urlPath)
                        .addHeader("Content-Length", String.valueOf(body.contentLength()))
                        .post(body)
                        .build();
                RestResponse restResponse = ResponseHandler.doCall(simpleHttpClient.getClient(), request);
                count++;
            }
            try {
                //Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(count > MAX) {
                break;
            }
        }
    }
}
