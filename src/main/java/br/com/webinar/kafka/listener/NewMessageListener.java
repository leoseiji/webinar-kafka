package br.com.webinar.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
public class NewMessageListener {

    private static Logger log = LoggerFactory.getLogger(NewMessageListener.class);

    private final CountDownLatch latch = new CountDownLatch(3);

    @KafkaListener(topics = "newMessageTopic")
    public void listen(ConsumerRecord<?, ?> cr) throws InterruptedException {
        log.info("Consumido: " + cr.value());
        log.info(cr.toString());
        log.info("Processando: " + cr.value());
        latch.await(20, TimeUnit.SECONDS);
        log.info("Processamento encerrado: " + cr.value());
    }

}
