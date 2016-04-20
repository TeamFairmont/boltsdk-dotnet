using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using BoltSDK;

namespace AMQP_Bolt_Test_Console
{
    class Program
    {
        
        static void Main(string[] args)
        {
            Console.Write("Enter AMQP IP: ");
            var ip = Console.ReadLine();
            if (ip == "")
                ip = "192.168.254.3";

            //connect
            try
            {
                var factory = new ConnectionFactory() { Uri = "amqp://guest:guest@" + ip };
                using (var connection = factory.CreateConnection())
                using (var channel = WorkerTools.CreateChannel(connection))
                {
                    //setup first command, which returns a new consumer
                    var consumer = WorkerTools.ConsumeCommand("parseKeywords", channel);
                    //setup another command, and add those to the already created consumer. 
                    //NOTE: its possible to have each command registered to a new consumer and dequeue them separately
                    WorkerTools.ConsumeCommand("fetchPage", channel, consumer);
                    
                    Console.WriteLine("MQ Connection success");

                    while (true)
                    {
                        //pull in the next command
                        var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                        Console.WriteLine("Command In: " + ea.RoutingKey);

                        //perform prep work via the sdk and get the JSON payload
                        var payload = WorkerTools.StartWork(ea);

                        try
                        {   
                            //do work
                            if (ea.RoutingKey == "parseKeywords")
                            {
                                var html = payload.SelectToken("return_value.html").ToString();

                                Console.WriteLine(" [x] Received HTML: " + html);

                                var kw = new List<string>();
                                kw.Add("abc");
                                kw.Add("123");

                                payload["return_value"]["keywords"] = new JArray(kw.ToArray());
                                Console.WriteLine(" [x] Keywords Parsed: " + payload["return_value"]["keywords"].ToString());
                            }
                            else if (ea.RoutingKey == "fetchPage")
                            {
                                payload["return_value"]["html"] = "C# Worker Page Content";
                            }
                        }
                        catch (Exception err)
                        {
                            Console.WriteLine("[x] Error: " + err.ToString());
                        }
                        finally
                        {
                            //send result back to bolt engine 
                            WorkerTools.FinishWork(channel, ea, payload);
                            Console.WriteLine("Command Out: " + ea.RoutingKey);
                        }
                    };
                }
            }
            catch (Exception err)
            {
                Console.WriteLine("MQ Connection error: "+err.ToString());
            }
        }
    }
}
