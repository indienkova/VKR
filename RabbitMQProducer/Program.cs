using AutoFixture;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RabbitMQProducer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var fixture = new Fixture();

            var factory = new ConnectionFactory
            {
                Uri = new Uri("amqp://admin:password@127.0.0.1:5672")
            };

            var context = new hadoopuserContext();

            var clients = context.Clients.ToList();
            var products = context.Products.ToList();

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var id = 1;
                while(true)
            {
                var rnd = new Random();
                var request = new RequestModel()
                {
                    Id = id,
                    Fullname = clients[rnd.Next(0, 100)].Fullname,
                    Product = products[rnd.Next(0, 10)].Name,
                    ReqDate = DateTime.UtcNow.AddDays(rnd.Next(-210, 0)).ToShortDateString()
                };
                id++;
                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request));
                channel.QueueDeclare(
                    "demo-queue",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                channel.BasicPublish("", "demo-queue", null, body);
                Thread.Sleep(50);
            }
            }
        }
    }
}
