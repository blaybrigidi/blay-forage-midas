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

    private final List<Transaction> receivedTransactions = new ArrayList<>();

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        receivedTransactions.add(transaction);
        logger.info("Received transaction #{}: {}", receivedTransactions.size(), transaction);

        // Log first 4 transactions with special debug info
        if (receivedTransactions.size() <= 4) {
            logger.warn("**DEBUG** Transaction #{}: Amount = {}", receivedTransactions.size(), transaction.getAmount());
        }

        // For debugging - log summary after first 4
        if (receivedTransactions.size() == 4) {
            logger.warn("**DEBUG SUMMARY** First 4 transaction amounts:");
            for (int i = 0; i < 4; i++) {
                logger.warn("**DEBUG** Transaction {}: Amount = {}", i + 1, receivedTransactions.get(i).getAmount());
            }
        }

        // Validate and process transaction
        processTransaction(transaction);

        // Log waldorf's balance after each transaction
        logWaldorfBalance();
    }

    private void processTransaction(Transaction transaction) {
        try {
            // 1. Validate sender exists
            UserRecord sender = userRepository.findById(transaction.getSenderId());
            if (sender == null) {
                logger.warn("Transaction rejected: Invalid senderId {}", transaction.getSenderId());
                return;
            }

            // 2. Validate recipient exists
            UserRecord recipient = userRepository.findById(transaction.getRecipientId());
            if (recipient == null) {
                logger.warn("Transaction rejected: Invalid recipientId {}", transaction.getRecipientId());
                return;
            }

            // 3. Validate sender has sufficient balance
            if (sender.getBalance() < transaction.getAmount()) {
                logger.warn("Transaction rejected: Insufficient balance. Sender {} has {}, needs {}",
                        sender.getName(), sender.getBalance(), transaction.getAmount());
                return;
            }

            // 4. Process valid transaction
            // Update balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            // Save updated user records
            userRepository.save(sender);
            userRepository.save(recipient);

            // Create and save transaction record
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRepository.save(transactionRecord);

            logger.info("Transaction processed successfully: {} -> {} amount {}",
                    sender.getName(), recipient.getName(), transaction.getAmount());
            logger.info("New balances: {} = {}, {} = {}",
                    sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());

        } catch (Exception e) {
            logger.error("Error processing transaction: {}", transaction, e);
        }
    }

    private void logWaldorfBalance() {
        try {
            UserRecord waldorf = userRepository.findByName("waldorf");
            if (waldorf != null) {
                logger.warn("🏦 WALDORF BALANCE: {} (rounded down: {})",
                        waldorf.getBalance(), (int) Math.floor(waldorf.getBalance()));
            }
        } catch (Exception e) {
            logger.error("Error checking waldorf balance", e);
        }
    }

    public List<Transaction> getReceivedTransactions() {
        return new ArrayList<>(receivedTransactions);
    }
}