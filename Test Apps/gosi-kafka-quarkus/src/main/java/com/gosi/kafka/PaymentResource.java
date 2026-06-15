package com.gosi.kafka;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
