package com.example.demo.service;

import com.example.demo.contract.MessageDTO;
import com.example.demo.kafka.KafkaProducer;
import org.springframework.stereotype.Service;

@Service
public class MessageService {

    private final KafkaProducer producer;

    public MessageService(KafkaProducer producer) {
        this.producer = producer;
    }

    public void processMessage(MessageDTO dto) {
        producer.sendMessage(dto.getContent());
    }

    public void processUserMessage(String msg) {
        producer.sendMessage(msg);
    }
}
