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
    
    // In a production system, this would point to an S3 bucket or blob storage containing the weights.
    // For this prototype, we will pass weights via Kafka and track the version here.
    private String modelReferencePath;
    
    private LocalDateTime createdAt;
    
    private boolean isActive;

    public GlobalModelVersion() {
        this.createdAt = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public int getVersionNumber() { return versionNumber; }
    public void setVersionNumber(int versionNumber) { this.versionNumber = versionNumber; }
    public String getModelReferencePath() { return modelReferencePath; }
    public void setModelReferencePath(String modelReferencePath) { this.modelReferencePath = modelReferencePath; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
}
