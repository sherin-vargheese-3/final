package com.example.demo.controller;

import com.example.demo.contract.MessageDTO;
import com.example.demo.service.MessageService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/messages")
@AllArgsConstructor
public class MessageController {

    private final MessageService messageService;

    @PostMapping
    public ResponseEntity<String> send(@RequestBody MessageDTO dto) {
        messageService.processMessage(dto);
        return ResponseEntity.ok("Message sent to Kafka");
    }
}
