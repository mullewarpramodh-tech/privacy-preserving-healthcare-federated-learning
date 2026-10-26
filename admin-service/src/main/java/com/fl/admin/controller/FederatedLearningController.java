package com.fl.admin.controller;

import com.fl.admin.config.KafkaConfig;
import com.fl.admin.model.GlobalModelVersion;
import com.fl.admin.repository.GlobalModelVersionRepository;
import com.fl.admin.service.FederatedAggregationService;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/fl")
public class FederatedLearningController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final GlobalModelVersionRepository modelVersionRepository;
    private final FederatedAggregationService aggregationService;

    public FederatedLearningController(KafkaTemplate<String, Object> kafkaTemplate,
                                       GlobalModelVersionRepository modelVersionRepository,
                                       FederatedAggregationService aggregationService) {
        this.kafkaTemplate = kafkaTemplate;
        this.modelVersionRepository = modelVersionRepository;
        this.aggregationService = aggregationService;
    }

    @PostMapping("/broadcast")
    public ResponseEntity<?> broadcastGlobalModel() {
        int round = aggregationService.getCurrentRound();
        
        Map<String, Object> globalModelEvent = Map.of(
            "version", round,
            "status", "START_TRAINING",
            "message", "Global Model Round #" + round + " broadcasted to all hospitals."
        );

        try {
            kafkaTemplate.send(KafkaConfig.TOPIC_GLOBAL_MODEL, globalModelEvent).get();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", "Kafka Broker unreachable: " + e.getMessage()));
        }

        return ResponseEntity.ok(Map.of(
            "message", "Global Model Round #" + round + " broadcasted successfully via Kafka!",
            "round", round
        ));
    }

    @GetMapping("/versions")
    public ResponseEntity<List<GlobalModelVersion>> getModelVersions() {
        return ResponseEntity.ok(modelVersionRepository.findAll());
    }

    @GetMapping("/latest-model")
    public ResponseEntity<?> getLatestModelVersion() {
        return modelVersionRepository.findTopByOrderByVersionNumberDesc()
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.ok().build());
    }
}
