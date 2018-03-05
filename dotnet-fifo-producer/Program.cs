using System;
using System.Collections.Generic;
using System.Linq;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Bogus;
using Newtonsoft.Json;

namespace dotnet_fifo_producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var num_messages = int.Parse(args[0]);
            var queue_url = args[1];
            var ids = new int[] {0, 0, 0, 0};
            System.Console.WriteLine($"Sending {num_messages} messages to SQS queue {queue_url}");
            var sqs = new AmazonSQSClient(); //need keys and region
            var testMessage = new Faker<TestMessage>()
                .RuleFor(u => u.client_id, f => f.Random.Number(1,4))//"1")
                .RuleFor(u => u.id, (f, u) => ids[u.client_id-1]++)
                .RuleFor(u => u.member_id, f => f.Random.Number(1, 1000000).ToString())
                .FinishWith((f, u) => System.Console.WriteLine($"Message created for client_id {u.client_id}"));

            try
            {
                var messages = new List<TestMessage>();
                for (int i = 0; i < num_messages; i++)
                {
                    var message = testMessage.Generate();
                    messages.Add(message);

                    if (messages.Count == 10)
                    {
                        var result = sqs.SendMessageBatchAsync(new SendMessageBatchRequest{
                            QueueUrl = queue_url,
                            Entries = messages.Select(x => new SendMessageBatchRequestEntry{
                                Id = Guid.NewGuid().ToString(),
                                MessageBody = JsonConvert.SerializeObject(x),
                                MessageGroupId = x.client_id.ToString(),
                                MessageAttributes = new Dictionary<string, MessageAttributeValue>()
                            }).ToList()
                        }).Result;
                        if (result.Failed.Any()) {
                            foreach (var entry in result.Failed)
                            {
                                System.Console.WriteLine($"Item {entry.Id} failed: {entry.Message}");
                            }
                        }
                        messages.Clear();
                    }
                }
            }
            catch (System.Exception)
            {
                
                throw;
            }
        }

        public class TestMessage
        {
            public int client_id {get;set;}
            public int id {get;set;}
            public string member_id {get;set;}
        }
    }
}
