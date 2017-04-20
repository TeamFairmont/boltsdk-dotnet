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
        static Random rand = new Random(); // For random sleep time to test parallel example workers
        
        static void Main(string[] args)
        {
            var cmd = "messageDotNet";
            var secondCmd = "addOrder";
            var ip = "192.168.1.14";

            // Prepare the MQ connection to be passed to the workers
            var factory = new ConnectionFactory() { Uri = "amqp://dev:dev@" + ip };
            using (var connection = factory.CreateConnection())
            {
                //run worker
                WorkerTools.RunWork(secondCmd, mySecondWorker, connection);
                WorkerTools.RunWork(cmd, myWorker, connection);
                Thread.Sleep(Timeout.Infinite);
            }
        }
        // myWorker is an example worker to help demonstrate the boltSDK-dotnet
        public static JObject myWorker(JObject payload)
        {
            var message = payload.SelectToken("initial_input.message").ToString();
            var msg = new List<string>();
            msg.Add("Message1");
            msg.Add("Message2");
            payload["return_value"]["message"] = new JArray(msg.ToArray());
            Thread.Sleep(rand.Next(1000, 10000));
            Console.WriteLine("id: " + payload["id"]);
            Console.WriteLine(payload["return_value"]);
            return payload;
        }
        // mySecondWorker is an example worker to help demonstrate the boltSDK-dotnet
        public static JObject mySecondWorker(JObject payload)
        {
            var message = payload.SelectToken("initial_input.message").ToString();
            var msg = new List<string>();
            msg.Add("Pertinent Data");
            msg.Add("Important Infomation2");
            payload["return_value"]["ImparativeReaction"] = new JArray(msg.ToArray());

            Thread.Sleep(rand.Next(10, 100));
            Console.WriteLine("id: " + payload["id"]);
            Console.WriteLine("call_in: " + payload["call_in"]);
            Console.WriteLine(payload["return_value"]);
            return payload;
        }
    }
}
