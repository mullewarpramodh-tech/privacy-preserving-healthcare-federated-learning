package com.fl.admin.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/fl")
public class FederatedLearningController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public FederatedLearningController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/broadcast")
    public ResponseEntity<?> broadcastGlobalModel() {
        // Create a mock payload representing the Global Model
        Map<String, Object> globalModelEvent = Map.of(
            "version", 1,
            "status", "START_TRAINING",
            "message", "Initial Global Model broadcasted to all hospitals."
        );

        // Broadcast to the Kafka topic synchronously to ensure it flushes
        try {
            kafkaTemplate.send("global-model-updates", globalModelEvent).get();
            System.out.println("KAFKA: Successfully flushed message to broker!");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(Map.of("error", "Kafka Broker unreachable: " + e.getMessage()));
        }

        return ResponseEntity.ok(Map.of("message", "Global Model successfully broadcasted via Kafka!"));
    }
}
