package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {

    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    @Autowired
    private RestTemplate restTemplate;

    public float getIncentiveAmount(Transaction transaction) {
        try {
            logger.info("Calling incentive API for transaction: {}", transaction);

            Incentive response = restTemplate.postForObject(
                    INCENTIVE_API_URL,
                    transaction,
                    Incentive.class);

            if (response != null) {
                logger.info("Received incentive amount: {}", response.getAmount());
                return response.getAmount();
            } else {
                logger.warn("Received null response from incentive API");
                return 0.0f;
            }

        } catch (Exception e) {
            logger.warn("Error calling incentive API, using fallback incentive of 0: {}", e.getMessage());
            return 0.0f; // Default to 0 incentive on error
        }
    }
}