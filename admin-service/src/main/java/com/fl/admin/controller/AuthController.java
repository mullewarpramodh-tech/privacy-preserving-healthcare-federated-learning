package com.fl.admin.controller;

import com.fl.admin.model.Hospital;
import com.fl.admin.repository.HospitalRepository;
import com.fl.admin.security.JwtUtil;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final HospitalRepository hospitalRepository;
    private final JwtUtil jwtUtil;
    private final PasswordEncoder passwordEncoder;

    public AuthController(HospitalRepository hospitalRepository, JwtUtil jwtUtil, PasswordEncoder passwordEncoder) {
        this.hospitalRepository = hospitalRepository;
        this.jwtUtil = jwtUtil;
        this.passwordEncoder = passwordEncoder;
    }

    @PostMapping("/register")
    public ResponseEntity<?> registerHospital(@RequestBody Hospital hospital) {
        if (hospitalRepository.findByEmail(hospital.getEmail()).isPresent()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Email already registered"));
        }
        
        // Secure password with BCrypt hashing before storing in Neon PostgreSQL
        hospital.setPassword(passwordEncoder.encode(hospital.getPassword()));
        hospitalRepository.save(hospital);
        
        return ResponseEntity.ok(Map.of(
            "message", "Hospital registered successfully", 
            "hospitalId", hospital.getId()
        ));
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody Map<String, String> credentials) {
        String email = credentials.get("email");
        String password = credentials.get("password");

        if (email == null || password == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Email and password are required"));
        }

        Optional<Hospital> hospitalOpt = hospitalRepository.findByEmail(email);
        
        if (hospitalOpt.isPresent() && passwordEncoder.matches(password, hospitalOpt.get().getPassword())) {
            String token = jwtUtil.generateToken(email);
            return ResponseEntity.ok(Map.of(
                "token", token,
                "hospitalId", hospitalOpt.get().getId(),
                "hospitalName", hospitalOpt.get().getName()
            ));
        }
        
        return ResponseEntity.status(401).body(Map.of("error", "Invalid credentials"));
    }
}
