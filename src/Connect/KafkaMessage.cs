using System;
using Confluent.Kafka;

namespace PipServices3.Kafka.Connect
{
    public class KafkaMessage
    {
        public Message<byte[], byte[]> Message { get; set; }
        public IConsumer<byte[], byte[]> Consumer { get; set; }
        public ConsumeResult<byte[], byte[]> Result { get; set; }
    }
}
