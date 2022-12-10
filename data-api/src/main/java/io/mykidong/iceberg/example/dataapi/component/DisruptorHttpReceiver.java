package io.mykidong.iceberg.example.dataapi.component;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.mykidong.iceberg.example.dataapi.domain.EventLog;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorHttpReceiver extends Receiver<EventLog> {

    private static Logger LOG = LoggerFactory.getLogger(DisruptorHttpReceiver.class);

    private ObjectMapper mapper = new ObjectMapper();


    public DisruptorHttpReceiver() {
        super(StorageLevel.MEMORY_ONLY());
    }


    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }


    public void receive() {
        try {
            LOG.info("ready to receive events...");
            CloseableHttpClient httpClient = HttpClients.createMinimal();
            HttpGet httpGet = new HttpGet(String.format("http://127.0.0.1:%s/event_log", DisruptorHttpServer.PORT));
            while (!isStopped()) {
                String response = httpClient.execute(httpGet, httpResponse -> {
                    return EntityUtils.toString(httpResponse.getEntity());
                });
                if(response.trim().equals("")) {
                    Thread.sleep(500);
                    continue;
                }
                EventLog eventLog = mapper.readValue(response, EventLog.class);
                store(eventLog);
            }
            restart("restart receiver...");
        } catch(Exception e) {
            e.printStackTrace();
            // restart if there is any other error
            restart("Error receiving data", e);
        }
    }
    
    

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }
}
