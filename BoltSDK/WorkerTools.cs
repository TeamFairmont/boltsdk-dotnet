using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using Newtonsoft.Json.Linq;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;

namespace BoltSDK
{
    /// <summary>
    ///  WorkerTools contains functions to make connecting to an MQ 
    ///  and registering bolt commands simple and consistent.
    /// </summary>
    public class WorkerTools
    {

        public static void RunWork(string cmd, Func<JObject, JObject> workerFunc, IModel channel)
        {
            var consumer = ConsumeCommand(cmd, channel);
            while (true)
            {
                var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                Task t = Task.Run(() =>
                {
                    try
                    {
                        Console.WriteLine("thread beginning");
                        var payload = WorkerTools.StartWork(ea);
                        try
                        {
                            if (ea.RoutingKey == "messageDotNet")
                            {
                                workerFunc(payload);
                            }
                            else if (ea.RoutingKey == "fetchPage")
                            {
                                payload["return_value"]["html"] = "C# Worker Page Content";
                            }
                            Thread.Sleep(500);
                        }
                        catch (Exception err)
                        {
                            Console.WriteLine("[x] Error: " + err.ToString());
                        }
                        finally
                        {
                            FinishWork(channel, ea, payload);
                        }
                        Console.WriteLine("thread ending");
                    }
                    catch (Exception err)
                    {
                        Console.WriteLine("MQ Connection error: " + err.ToString());
                    }
                });
            };
        }

        /// <summary>
        /// CreateChannel takes an active MQ connection, and setups up
        /// a new channel with default bolt QoS settings 
        /// </summary>
        public static IModel CreateChannel(IConnection con)
        {
            var channel = con.CreateModel();
            channel.QueueDeclare(
                queue: "",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );
            channel.BasicQos(0, 10, false);

            return channel;
        }

        /// <summary>
        ///  ConsumeCommand registers a command name to process with the current channel.
        ///  If no consumer is passed, a new one will be created. Otherwise, the command
        ///  will be added to the existing consumer.
        /// </summary>
        public static QueueingBasicConsumer ConsumeCommand(string cmdName, IModel channel, QueueingBasicConsumer consumer = null )
        {
            if (consumer==null)
            {
                consumer = new QueueingBasicConsumer(channel);
            }
            channel.QueueDeclare(
                queue: cmdName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );
            channel.BasicConsume(queue: cmdName, noAck: false, consumer: consumer);
            return consumer;
        }

        /// <summary>
        ///  StartWork sets up, verifies, and returns the bolt payload for this 
        ///  command. It should be called immediately following consumer.Queue.Dequeue()
        /// </summary>
        public static JObject StartWork(BasicDeliverEventArgs ea)
        {
            var body = ea.Body;
            var message = Encoding.UTF8.GetString(body);

            var payload = JObject.Parse(message);
            return payload;
        }

        /// <summary>
        ///  FinishWork verifies the new payload and sends it back up to the bolt engine.
        ///  This needs to be called after all work is done for the current command.
        /// </summary>
        public static void FinishWork(IModel channel, BasicDeliverEventArgs ea, JObject payload)
        {
            IBasicProperties props = channel.CreateBasicProperties();
            props.ContentType = "text/json";
            props.CorrelationId = ea.BasicProperties.CorrelationId;

            channel.BasicPublish(
                exchange: "",
                routingKey: ea.BasicProperties.ReplyTo,
                basicProperties: props,
                body: Encoding.UTF8.GetBytes(payload.ToString())
            );

            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        }
    }
}
