package com.fl.admin.controller;

import com.fl.admin.model.Hospital;
import com.fl.admin.repository.HospitalRepository;
import com.fl.admin.security.JwtUtil;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final HospitalRepository hospitalRepository;
    private final JwtUtil jwtUtil;

    public AuthController(HospitalRepository hospitalRepository, JwtUtil jwtUtil) {
        this.hospitalRepository = hospitalRepository;
        this.jwtUtil = jwtUtil;
    }

    @PostMapping("/register")
    public ResponseEntity<?> registerHospital(@RequestBody Hospital hospital) {
        if (hospitalRepository.findByEmail(hospital.getEmail()).isPresent()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Email already registered"));
        }
        hospitalRepository.save(hospital);
        return ResponseEntity.ok(Map.of("message", "Hospital registered successfully", "hospitalId", hospital.getId()));
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody Map<String, String> credentials) {
        String email = credentials.get("email");
        String password = credentials.get("password");

        Optional<Hospital> hospitalOpt = hospitalRepository.findByEmail(email);
        
        if (hospitalOpt.isPresent() && hospitalOpt.get().getPassword().equals(password)) {
            String token = jwtUtil.generateToken(email);
            return ResponseEntity.ok(Map.of("token", token));
        }
        
        return ResponseEntity.status(401).body(Map.of("error", "Invalid credentials"));
    }
}
