using System;
using System.Collections.Generic;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;

namespace dotnet_fifo_consumer
{
    class Program
    {
        static Random random {get;} = new Random();

        static void Main(string[] args)
        {
            var batch_size = int.Parse(args[0]);
            var queue_url = args[1];            
            System.Console.WriteLine($"Reading {batch_size} messages at a time from SQS queue {queue_url}");            
            var sqs = new AmazonSQSClient(); // need keys and region
            try
            {
                while(true)
                {
                    var receive_message_request = new ReceiveMessageRequest
                    {
                        AttributeNames = new List<string>() { "All" },
                        MaxNumberOfMessages = batch_size,
                        QueueUrl = queue_url,
                        WaitTimeSeconds = 20
                    };

                    var result = sqs.ReceiveMessageAsync(receive_message_request).Result;
                    
                    foreach (var message in result.Messages)
                    {
                        if (process_message(message))
                        {
                            sqs.DeleteMessageAsync(new DeleteMessageRequest
                            {
                                QueueUrl = queue_url,
                                ReceiptHandle = message.ReceiptHandle
                            });
                        }
                        else
                        {
                            sqs.ChangeMessageVisibilityAsync(queue_url, message.ReceiptHandle, 10);
                        }
                    }
                }
            }
            catch (System.Exception)
            {
                
                throw;
            }
        }

        static bool process_message(Message message)
        {
            var failed = random.Next(1, 30) == 1;
            dynamic json = JsonConvert.DeserializeObject(message.Body);
            if (failed)
            {
                System.Console.WriteLine($"{json.client_id}:{json.id} failed {DateTime.Now}");
            }
            else 
            {
                System.Console.WriteLine($"{json.client_id}:{json.id}");
            }
            return !failed;
        }
    }
}
