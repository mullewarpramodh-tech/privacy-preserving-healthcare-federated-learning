package com.fl.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AdminServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(AdminServiceApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(AdminServiceApplication.class, args);
    }

    @Bean
    public CommandLineRunner printStartupBanner() {
        return args -> {
            log.info("=================================================================");
            log.info("  🏥 FEDERATED HEALTHCARE AI ORCHESTRATOR IS ONLINE");
            log.info("  👉 Web Console:   http://localhost:8080");
            log.info("  👉 Kafka Broker:  localhost:9092");
            log.info("=================================================================");
        };
    }
}