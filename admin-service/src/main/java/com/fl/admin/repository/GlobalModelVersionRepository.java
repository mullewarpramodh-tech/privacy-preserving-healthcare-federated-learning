package com.fl.admin.repository;

import com.fl.admin.model.GlobalModelVersion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface GlobalModelVersionRepository extends JpaRepository<GlobalModelVersion, Long> {
    Optional<GlobalModelVersion> findTopByOrderByVersionNumberDesc();
}
