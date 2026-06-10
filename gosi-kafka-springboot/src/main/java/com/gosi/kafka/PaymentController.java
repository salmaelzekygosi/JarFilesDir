package com.gosi.kafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentProducerService producerService;

    public PaymentController(PaymentProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<Payment> createPayment(@RequestBody Payment payment) {
        if (payment.getId() == null || payment.getId().isBlank()) {
            payment.setId(UUID.randomUUID().toString());
        }
        
        producerService.sendPayment(payment);
        return ResponseEntity.accepted().body(payment);
    }
}
