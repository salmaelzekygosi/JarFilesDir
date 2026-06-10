using System;
using System.Threading.Tasks;
using Gosi.Kafka.Services;
using Microsoft.AspNetCore.Mvc;

namespace Gosi.Kafka.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class PaymentsController : ControllerBase
    {
        private readonly KafkaProducerService _producerService;

        public PaymentsController(KafkaProducerService producerService)
        {
            _producerService = producerService;
        }

        [HttpPost]
        public async Task<IActionResult> CreatePayment([FromBody] Payment payment)
        {
            if (string.IsNullOrEmpty(payment.Id))
            {
                payment.Id = Guid.NewGuid().ToString();
            }

            await _producerService.SendPaymentAsync(payment);
            return Accepted(payment);
        }
    }
}
