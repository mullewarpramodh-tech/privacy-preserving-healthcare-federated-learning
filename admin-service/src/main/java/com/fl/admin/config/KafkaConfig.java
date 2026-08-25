package com.fl.admin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

@Configuration
public class KafkaConfig {

    @Bean
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
        EmbeddedKafkaZKBroker broker = new EmbeddedKafkaZKBroker(1, true, 1, "global-model-updates", "hospital-local-weights");
        broker.brokerProperty("listeners", "PLAINTEXT://127.0.0.1:9092");
        broker.brokerProperty("advertised.listeners", "PLAINTEXT://127.0.0.1:9092");
        broker.kafkaPorts(9092);
        return broker;
    }
}
