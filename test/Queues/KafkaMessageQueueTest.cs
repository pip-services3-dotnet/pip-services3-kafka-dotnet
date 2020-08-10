using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using System;
using Xunit;

namespace PipServices3.Kafka.Queues
{
    public class KafkaMessageQueueTest: IDisposable
    {
        private bool _enabled;
        private KafkaMessageQueue _queue;
        private MessageQueueFixture _fixture;

        public KafkaMessageQueueTest()
        {
            var KAFKA_ENABLED = Environment.GetEnvironmentVariable("KAFKA_ENABLED") ?? "true";
            var KAFKA_URI = Environment.GetEnvironmentVariable("KAFKA_URI");
            var KAFKA_HOST = Environment.GetEnvironmentVariable("KAFKA_HOST") ?? "localhost";
            var KAFKA_PORT = Environment.GetEnvironmentVariable("KAFKA_PORT") ?? "9092";
            var KAFKA_TOPIC = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test";
            var KAFKA_USER = Environment.GetEnvironmentVariable("KAFKA_USER") ?? "user";
            var KAFKA_PASS = Environment.GetEnvironmentVariable("KAFKA_PASS") ?? "pass123";

            _enabled = BooleanConverter.ToBoolean(KAFKA_ENABLED);

            if (_enabled)
            {
                _queue = new KafkaMessageQueue();
                _queue.Configure(ConfigParams.FromTuples(
                    "topic.name", KAFKA_TOPIC,
                    "topic.num_partitions", 1,
                    "topic.replication_factor", -1,

                    "consumer.group_id", "custom-group",
                    
                    "connection.uri", KAFKA_URI,
                    "connection.host", KAFKA_HOST,
                    "connection.port", KAFKA_PORT,
                    "credential.username", KAFKA_USER,
                    "credential.password", KAFKA_PASS,
                    "credential.protocol", ""
                ));

                _queue.OpenAsync(null).Wait();
                _queue.ClearAsync(null).Wait();

                _fixture = new MessageQueueFixture(_queue);
            }
        }

        public void Dispose()
        {
            if (_queue != null)
                _queue.CloseAsync(null).Wait();
        }

        [Fact]
        public void TestKafkaSendReceiveMessage()
        {
            if (_enabled)
                _fixture.TestSendReceiveMessageAsync().Wait();
        }

        [Fact]
        public void TestKafkaReceiveSendMessage()
        {
            if (_enabled)
                _fixture.TestReceiveSendMessageAsync().Wait();
        }

        [Fact]
        public void TestKafkaReceiveAndComplete()
        {
            if (_enabled)
                _fixture.TestReceiveAndCompleteMessageAsync().Wait();
        }

        [Fact]
        public void TestKafkaReceiveAndAbandon()
        {
            if (_enabled)
                _fixture.TestReceiveAndAbandonMessageAsync().Wait();
        }

        [Fact]
        public void TestKafkaSendPeekMessage()
        {
            if (_enabled)
                _fixture.TestSendPeekMessageAsync().Wait();
        }

        [Fact]
        public void TestKafkaPeekNoMessage()
        {
            if (_enabled)
                _fixture.TestPeekNoMessageAsync().Wait();
        }

        [Fact]
        public void TestKafkaOnMessage()
        {
            if (_enabled)
                _fixture.TestOnMessageAsync().Wait();
        }

        //[Fact]
        //public void TestKafkaMoveToDeadMessage()
        //{
        //    if (_enabled)
        //        _fixture.TestMoveToDeadMessageAsync().Wait();
        //}

        [Fact]
        public void TestKafkaMessageCount()
        {
            if (_enabled)
                _fixture.TestMessageCountAsync().Wait();
        }
    }
}
