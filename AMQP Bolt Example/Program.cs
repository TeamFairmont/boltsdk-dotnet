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
        static Random rand = new Random();
        public static JObject myWorker(JObject p)
        {
            var message = p.SelectToken("initial_input.message").ToString();
            var msg = new List<string>();
            msg.Add("Message1");
            msg.Add("Message2");
            p["return_value"]["message"] = new JArray(msg.ToArray());
            Thread.Sleep(rand.Next(10,1000));
            Console.WriteLine("id: "+p["id"]);
            Console.WriteLine("call_in: "+p["call_in"]);
            Console.WriteLine(p["return_value"]);
            return p;
        }
        
        static void Main(string[] args)
        {
            var cmd = "messageDotNet";
            var ip = "192.168.1.14";

            //prepare the mq channel
            var factory = new ConnectionFactory() { Uri = "amqp://dev:dev@" + ip };
            using (var connection = factory.CreateConnection())
            using (var channel = WorkerTools.CreateChannel(connection))
            {
                //run worker
                WorkerTools.RunWork(cmd, myWorker, channel);
                //Wait forever
                Thread.Sleep(Timeout.Infinite);
            }
        }
    }
}
