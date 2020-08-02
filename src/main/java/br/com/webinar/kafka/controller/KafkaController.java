package br.com.webinar.kafka.controller;

import br.com.webinar.kafka.model.MessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
public class KafkaController {

    private static Logger log = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    private KafkaTemplate<String, String> template;

    @PostMapping("/insert")
    public ResponseEntity greeting(@RequestBody MessageRequest messageRequest) {
        log.info("Insert message: " + messageRequest.getMessage());
        this.template.send("newMessageTopic", messageRequest.getMessage());
        log.info("Inserido no kafka: " + messageRequest.getMessage());
        return ResponseEntity.ok("Inserido no kafka: " + messageRequest.getMessage());
    }
}
