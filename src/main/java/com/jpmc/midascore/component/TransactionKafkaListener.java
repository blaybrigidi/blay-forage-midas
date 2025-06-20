package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Component
public class TransactionKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionKafkaListener.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private IncentiveService incentiveService;

    private final List<Transaction> receivedTransactions = new ArrayList<>();

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        receivedTransactions.add(transaction);
        logger.info("Received transaction #{}: {}", receivedTransactions.size(), transaction);

        // Validate transaction
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null) {
            logger.warn("Transaction rejected: Invalid sender ID {}", transaction.getSenderId());
            return;
        }

        if (recipient == null) {
            logger.warn("Transaction rejected: Invalid recipient ID {}", transaction.getRecipientId());
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Transaction rejected: Insufficient balance. Sender {} has {}, needs {}",
                    sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Get incentive amount from API
        float incentiveAmount = incentiveService.getIncentiveAmount(transaction);

        // Update balances
        float newSenderBalance = sender.getBalance() - transaction.getAmount();
        float newRecipientBalance = recipient.getBalance() + transaction.getAmount() + incentiveAmount;

        sender.setBalance(newSenderBalance);
        recipient.setBalance(newRecipientBalance);

        // Save updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Create and save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(),
                incentiveAmount);
        transactionRepository.save(transactionRecord);

        logger.info("Transaction processed successfully: {} -> {} amount: {}, incentive: {}",
                sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);
        logger.info("New balances - {}: {}, {}: {}",
                sender.getName(), newSenderBalance, recipient.getName(), newRecipientBalance);

        // Log wilbur's balance specifically for debugging TaskFourTests
        UserRecord wilbur = userRepository.findByName("wilbur");
        if (wilbur != null) {
            logger.info("WILBUR BALANCE: {}", wilbur.getBalance());
        }

        // Debug first 4 transactions
        if (receivedTransactions.size() <= 4) {
            logger.debug("DEBUG - Transaction #{}: amount={}", receivedTransactions.size(), transaction.getAmount());
        }
    }

    public List<Transaction> getReceivedTransactions() {
        return new ArrayList<>(receivedTransactions);
    }
}