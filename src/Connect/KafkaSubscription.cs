using System;
using System.Threading;
using Confluent.Kafka;

namespace PipServices3.Kafka.Connect
{
    public class KafkaSubscription
    {
        public string Topic { get; set; }
        public string GroupId { get; set; }
        public IKafkaMessageListener Listener { get; set; }
        public IConsumer<byte[],byte[]> Handler { get; set; }
        public CancellationTokenSource Token { get; set; }
    }
}
