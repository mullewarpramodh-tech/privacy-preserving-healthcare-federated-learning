package com.fl.admin.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "global_model_versions")
public class GlobalModelVersion {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int versionNumber;
    
    // In production, points to an S3 object key or model registry artifact
    private String modelReferencePath;
    
    private Double globalAccuracy;
    
    private Integer participatingNodesCount;
    
    private LocalDateTime createdAt;
    
    private boolean isActive;

    public GlobalModelVersion() {
        this.createdAt = LocalDateTime.now();
    }

    public GlobalModelVersion(int versionNumber, Double globalAccuracy, int participatingNodesCount, String modelReferencePath) {
        this.versionNumber = versionNumber;
        this.globalAccuracy = globalAccuracy;
        this.participatingNodesCount = participatingNodesCount;
        this.modelReferencePath = modelReferencePath;
        this.createdAt = LocalDateTime.now();
        this.isActive = true;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public int getVersionNumber() { return versionNumber; }
    public void setVersionNumber(int versionNumber) { this.versionNumber = versionNumber; }
    public String getModelReferencePath() { return modelReferencePath; }
    public void setModelReferencePath(String modelReferencePath) { this.modelReferencePath = modelReferencePath; }
    public Double getGlobalAccuracy() { return globalAccuracy; }
    public void setGlobalAccuracy(Double globalAccuracy) { this.globalAccuracy = globalAccuracy; }
    public Integer getParticipatingNodesCount() { return participatingNodesCount; }
    public void setParticipatingNodesCount(Integer participatingNodesCount) { this.participatingNodesCount = participatingNodesCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
}
