package com.fl.admin.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fl.admin.config.KafkaConfig;
import com.fl.admin.model.GlobalModelVersion;
import com.fl.admin.repository.GlobalModelVersionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class FederatedAggregationService {

    private static final Logger log = LoggerFactory.getLogger(FederatedAggregationService.class);

    private final GlobalModelVersionRepository modelVersionRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${fl.min-clients-for-round:1}")
    private int minClientsForRound;

    // In-memory buffer storing client updates indexed by round number
    private final Map<Integer, List<HospitalUpdate>> roundBuffer = new ConcurrentHashMap<>();
    private final AtomicInteger currentRound = new AtomicInteger(1);

    public FederatedAggregationService(GlobalModelVersionRepository modelVersionRepository,
                                     KafkaTemplate<String, Object> kafkaTemplate) {
        this.modelVersionRepository = modelVersionRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public record HospitalUpdate(
        Long hospitalId,
        String hospitalName,
        int roundId,
        int sampleCount,
        double localAccuracy,
        double localLoss,
        List<Object> weights
    ) {}

    public record WeightedTensor(Object tensor, double weight) {}

    @KafkaListener(topics = KafkaConfig.TOPIC_LOCAL_WEIGHTS, groupId = "fl-admin-group")
    public void consumeLocalHospitalWeights(String messagePayload) {
        try {
            Map<String, Object> payload = objectMapper.readValue(messagePayload, new TypeReference<>() {});
            
            Long hospitalId = ((Number) payload.getOrDefault("hospitalId", 0L)).longValue();
            String hospitalName = (String) payload.getOrDefault("hospitalName", "Unknown Node");
            int roundId = ((Number) payload.getOrDefault("roundId", currentRound.get())).intValue();
            int sampleCount = ((Number) payload.getOrDefault("sampleCount", 100)).intValue();
            double localAccuracy = ((Number) payload.getOrDefault("localAccuracy", 0.0)).doubleValue();
            double localLoss = ((Number) payload.getOrDefault("localLoss", 0.0)).doubleValue();
            
            @SuppressWarnings("unchecked")
            List<Object> weights = (List<Object>) payload.get("weights");

            log.info("📥 [Kafka] Received weights from Hospital #{}: '{}' for Round #{} (Accuracy: {}%, Samples: {})",
                    hospitalId, hospitalName, roundId, String.format("%.2f", localAccuracy * 100), sampleCount);

            HospitalUpdate update = new HospitalUpdate(hospitalId, hospitalName, roundId, sampleCount, localAccuracy, localLoss, weights);

            roundBuffer.computeIfAbsent(roundId, k -> Collections.synchronizedList(new ArrayList<>())).add(update);

            List<HospitalUpdate> currentRoundUpdates = roundBuffer.get(roundId);
            if (currentRoundUpdates.size() >= minClientsForRound) {
                log.info("🎯 Quorum met ({} updates). Executing Federated Averaging (FedAvg)...", currentRoundUpdates.size());
                aggregateAndPublishNextRound(roundId, currentRoundUpdates);
            }

        } catch (Exception e) {
            log.error("❌ Failed to process hospital weights Kafka message: {}", e.getMessage(), e);
        }
    }

    /**
     * Executes the mathematical Federated Averaging (FedAvg) algorithm:
     * W_global = sum( (n_k / N_total) * W_k )
     */
    private synchronized void aggregateAndPublishNextRound(int roundId, List<HospitalUpdate> updates) {
        int totalSamples = updates.stream().mapToInt(HospitalUpdate::sampleCount).sum();
        if (totalSamples == 0) totalSamples = 1;

        // 1. Calculate weighted average accuracy
        double weightedAccuracy = 0.0;
        for (HospitalUpdate u : updates) {
            weightedAccuracy += ((double) u.sampleCount() / totalSamples) * u.localAccuracy();
        }

        // 2. Aggregate multi-dimensional tensor weights
        List<Object> aggregatedWeights = null;
        if (updates.get(0).weights() != null && !updates.get(0).weights().isEmpty()) {
            int numLayers = updates.get(0).weights().size();
            aggregatedWeights = new ArrayList<>(numLayers);

            for (int layerIdx = 0; layerIdx < numLayers; layerIdx++) {
                final int lIdx = layerIdx;
                List<WeightedTensor> layerTensors = new ArrayList<>();
                for (HospitalUpdate u : updates) {
                    double weightRatio = (double) u.sampleCount() / totalSamples;
                    layerTensors.add(new WeightedTensor(u.weights().get(lIdx), weightRatio));
                }
                aggregatedWeights.add(averageTensor(layerTensors));
            }
        }

        // 3. Persist new Global Model Version into Neon PostgreSQL
        int nextRound = roundId + 1;
        currentRound.set(nextRound);

        GlobalModelVersion newVersion = new GlobalModelVersion(
            nextRound,
            Math.round(weightedAccuracy * 10000.0) / 10000.0,
            updates.size(),
            "models/checkpoints/global_v" + nextRound + ".weights"
        );
        modelVersionRepository.save(newVersion);

        log.info("💾 [Neon DB] Persisted Global Model Version #{} with Aggregated Accuracy: {}%",
                nextRound, String.format("%.2f", weightedAccuracy * 100));

        // 4. Broadcast updated global model weights to Kafka topic 'global-model-updates'
        Map<String, Object> broadcastEvent = new HashMap<>();
        broadcastEvent.put("version", nextRound);
        broadcastEvent.put("status", "ROUND_COMPLETED");
        broadcastEvent.put("globalAccuracy", newVersion.getGlobalAccuracy());
        broadcastEvent.put("message", "Aggregated weights available for Round #" + nextRound);
        if (aggregatedWeights != null) {
            broadcastEvent.put("weights", aggregatedWeights);
        }

        kafkaTemplate.send(KafkaConfig.TOPIC_GLOBAL_MODEL, broadcastEvent);
        log.info("📢 [Kafka] Broadcasted Global Model Version #{} to topic '{}'", nextRound, KafkaConfig.TOPIC_GLOBAL_MODEL);

        // Clear round buffer to free memory
        roundBuffer.remove(roundId);
    }

    /**
     * Recursively computes the weighted average of arbitrary N-dimensional tensor arrays.
     */
    @SuppressWarnings("unchecked")
    private Object averageTensor(List<WeightedTensor> items) {
        if (items.isEmpty()) return null;
        Object first = items.get(0).tensor();

        if (first instanceof Number) {
            double sum = 0.0;
            for (WeightedTensor wt : items) {
                sum += wt.weight() * ((Number) wt.tensor()).doubleValue();
            }
            return sum;
        } else if (first instanceof List<?>) {
            List<?> firstList = (List<?>) first;
            int size = firstList.size();
            List<Object> averagedList = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                final int idx = i;
                List<WeightedTensor> subTensors = new ArrayList<>();
                for (WeightedTensor wt : items) {
                    List<?> currentList = (List<?>) wt.tensor();
                    subTensors.add(new WeightedTensor(currentList.get(idx), wt.weight()));
                }
                averagedList.add(averageTensor(subTensors));
            }
            return averagedList;
        }

        return first;
    }

    public int getCurrentRound() {
        return currentRound.get();
    }
}
