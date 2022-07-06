package com.example.kafkaproducer;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
    private final KafkaTemplate kafkaTemplate;

    private static final String TOPIC = "test";

    public KafkaController(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/publish")
    public ResponseEntity<?> publish (@RequestBody List<User> users) {
        try {
            for (User user : users) {
                kafkaTemplate.send(TOPIC, user);
            }
            return ResponseEntity.ok().body("Send successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.badRequest().body("Send failed");
        }
    }
}
