package com.example.kafka.controller;

import com.example.kafka.service.KafkaTopicService;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class ProduceController {

    private final KafkaTopicService topicService;

    public ProduceController(KafkaTopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping("/send")
    public ResponseEntity<String> produceNewMessage(RequestEntity<String> message) {
        topicService.sendMessage(message.getBody());
        return ResponseEntity.ok("successfully sent");
    }
}
