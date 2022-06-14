using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQProducer
{
    internal class RequestModel
    {
        public int Id { get; set; }
        public string Fullname { get; set; }
        public string Product { get; set; }
        public string ReqDate { get; set; }
    }
}
