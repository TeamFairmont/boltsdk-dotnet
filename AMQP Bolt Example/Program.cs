using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BoltSDK;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json.Linq;

namespace ConsoleApp2
{
    class Program
    {
        public static JObject myWorker(JObject p)
        {
            var message = p.SelectToken("initial_input.message").ToString();
            var msg = new List<string>();
            msg.Add("Message1");
            msg.Add("Message2");
            p["return_value"]["message"] = new JArray(msg.ToArray());
            return p;
        }
        
        static void Main(string[] args)
        {
            var cmd = "messageDotNet";
            var ip = "192.168.1.14";
            var factory = new ConnectionFactory() { Uri = "amqp://dev:dev@" + ip };
            using (var connection = factory.CreateConnection())
            using (var channel = WorkerTools.CreateChannel(connection))
            {
                WorkerTools.RunWork(cmd, myWorker, channel);
            }
        }
    }
}
