package com.example.demo.kafka;

import com.example.demo.model.Message;
import com.example.demo.repository.MessageRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private final MessageRepository messageRepository;

    public KafkaConsumer(MessageRepository repository) {
        this.messageRepository = repository;
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "message_group")
    public void listen(String content) {
        if (content.toLowerCase().contains("fail")) {   // Force failure if message contains the word "fail".
            throw new RuntimeException("Message processing failed - will route to DLT");
        }

        Message msg = new Message();
        msg.setContent(content);
        messageRepository.save(msg);
    }

    @KafkaListener(topics = "${kafka.topic.dlt-name}")
    public void handleDLT(String failedMessage) {
        System.out.println("DLT received: " + failedMessage);
    }
}
