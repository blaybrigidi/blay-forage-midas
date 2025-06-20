package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class KafkaListenerTest {
    static final Logger logger = LoggerFactory.getLogger(KafkaListenerTest.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Test
    void testKafkaListener() throws InterruptedException {
        // Send a few test transactions
        kafkaProducer.send("1, 2, 100.50");
        kafkaProducer.send("3, 4, 200.75");
        kafkaProducer.send("5, 6, 300.25");

        // Wait for messages to be processed
        Thread.sleep(3000);

        logger.info("Test completed - check logs for received transactions");
    }
}