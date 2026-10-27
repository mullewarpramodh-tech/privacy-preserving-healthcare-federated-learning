package com.fl.admin;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AdminServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminServiceApplication.class, args);
    }

    @Bean
    public CommandLineRunner printStartupBanner() {
        return args -> {
            System.out.println("\n" + "=".repeat(65));
            System.out.println("  🏥 FEDERATED HEALTHCARE AI ORCHESTRATOR IS ONLINE");
            System.out.println("=".repeat(65));
            System.out.println("  👉 Web Console:    http://localhost:8080");
            System.out.println("  👉 Kafka Broker:   localhost:9092");
            System.out.println("  👉 Spring Boot:    v" + SpringBootVersion.getVersion());
            System.out.println("=".repeat(65) + "\n");
        };
    }
}