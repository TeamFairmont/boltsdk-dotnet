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
    ///  and registering Bolt commands simple and consistent.
    /// </summary>
    public class WorkerTools
    {
        /// <summary>
        ///  RunWorker takes a command name, a worker function, and a connection
        /// </summary>
        public static void RunWork(string cmd, Func<JObject, JObject> workerFunc, IConnection connection)
        {
            // Start outer task, so the while loop does not block the next worker
            Task outerTask = Task.Run(()=> {
                // Create a disposable channel, limited to this scope
                using (var channel = CreateChannel(connection))
                {
                    // Prepare the consumer to recieve messages from the queue
                    var consumer = ConsumeCommand(cmd, channel);
                    // Infinite loop
                    while (true)
                    {
                        // Dequeue (retrieve messages from the queue), and get ea (event args) for processing.
                        // Block if nothing is on the queue.
                        var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                        // Start inner task so the work doesn't block
                        Task t = Task.Run(() =>
                        {
                            try
                            {
                                // Get the payload from ea, the queue's event arguments
                                var payload = StartWork(ea);
                                try
                                {
                                    workerFunc(payload);
                                }
                                catch (Exception err)
                                {
                                    Console.WriteLine("[x] Error: " + err.ToString());
                                }
                                finally // when finished run the FinishWork command to send acknowledgement to the queue and clean up
                                {
                                    FinishWork(channel, ea, payload);
                                }
                            }
                            catch (Exception err)
                            {
                                Console.WriteLine("MQ Connection error: " + err.ToString());
                            }
                        }); // end of inner task
                    }; // end while
                } // end using
            }); // end of outer task
            Console.WriteLine("worker started: "+cmd);
        }

        /// <summary>
        /// CreateChannel takes an active MQ connection, and sets up
        /// a new channel with default Bolt QoS settings 
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
        ///  ConsumeCommand registers a command name to process using the current channel.
        ///  If no consumer is passed, a new one will be created.
        ///   Otherwise, the command will be added to the existing consumer.
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
        ///  StartWork sets up, verifies, and returns the Bolt payload for this command.
        ///  It should be called immediately following consumer.Queue.Dequeue()
        /// </summary>
        public static JObject StartWork(BasicDeliverEventArgs ea)
        {
            var body = ea.Body;
            var message = Encoding.UTF8.GetString(body);

            var payload = JObject.Parse(message);
            return payload;
        }

        /// <summary>
        ///  FinishWork verifies the new payload and sends it back up to the Bolt engine.
        ///  This needs to be called after all work is completed for the current command.
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
