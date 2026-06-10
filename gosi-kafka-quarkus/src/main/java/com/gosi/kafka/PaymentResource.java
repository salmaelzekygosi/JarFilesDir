package com.gosi.kafka;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/payments")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class PaymentResource {

    @Inject
    PaymentProducer producer;

    @POST
    public Response createPayment(Payment payment) {
        if (payment.getId() == null || payment.getId().isBlank()) {
            payment.setId(java.util.UUID.randomUUID().toString());
        }
        
        producer.publishPayment(payment);
        return Response.accepted(payment).build();
    }
}
