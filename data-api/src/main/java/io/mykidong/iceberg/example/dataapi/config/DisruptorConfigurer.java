package io.mykidong.iceberg.example.dataapi.config;

import com.lmax.disruptor.dsl.Disruptor;
import io.mykidong.iceberg.example.dataapi.component.DisruptorConsumer;
import io.mykidong.iceberg.example.dataapi.component.DisruptorHttpReceiver;
import io.mykidong.iceberg.example.dataapi.component.DisruptorHttpServer;
import io.mykidong.iceberg.example.dataapi.domain.EventLog;
import io.mykidong.iceberg.example.dataapi.util.DisruptorCreator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ConcurrentLinkedQueue;

@Configuration
public class DisruptorConfigurer {

    @Bean
    public Disruptor<EventLog> eventLogDisruptor(){
        Disruptor<EventLog> putDisruptor =
                DisruptorCreator.singleton(DisruptorCreator.DISRUPTOR_NAME_PUT, EventLog.FACTORY, 1024, disruptorConsumer());

        return putDisruptor;
    }

    @Bean
    public DisruptorConsumer disruptorConsumer() {
        return new DisruptorConsumer(eventLogQueue());
    }

    @Bean
    public DisruptorHttpServer disruptorHttpServer() {
        return new DisruptorHttpServer(eventLogQueue());
    }


    @Bean
    public ConcurrentLinkedQueue<EventLog> eventLogQueue() {
        return new ConcurrentLinkedQueue<EventLog>();
    }

    @Bean
    public DisruptorHttpReceiver disruptorHttpReceiver() {
        return new DisruptorHttpReceiver();
    }
}
