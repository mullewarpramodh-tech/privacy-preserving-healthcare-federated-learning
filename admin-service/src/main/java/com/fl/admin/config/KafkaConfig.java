package com.fl.admin.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

@Configuration
@EnableKafka
public class KafkaConfig {

    public static final String TOPIC_GLOBAL_MODEL = "global-model-updates";
    public static final String TOPIC_LOCAL_WEIGHTS = "hospital-local-weights";

    @Bean
    public NewTopic globalModelTopic() {
        return TopicBuilder.name(TOPIC_GLOBAL_MODEL).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic localWeightsTopic() {
        return TopicBuilder.name(TOPIC_LOCAL_WEIGHTS).partitions(1).replicas(1).build();
    }

    /**
     * Development convenience: In-memory Embedded Kafka broker enables running
     * the system locally without needing a separate Docker or cloud Kafka container.
     */
    @Bean
    @ConditionalOnProperty(name = "fl.kafka.embedded", havingValue = "true", matchIfMissing = true)
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
    EmbeddedKafkaZKBroker broker = new EmbeddedKafkaZKBroker(1, true, 1);        broker.brokerProperty("listeners", "PLAINTEXT://127.0.0.1:9092");
        broker.brokerProperty("advertised.listeners", "PLAINTEXT://127.0.0.1:9092");
        broker.kafkaPorts(9092);
        return broker;
    }
}
