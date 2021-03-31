using PipServices3.Commons.Config;
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
            var KAFKA_SERVICE_URI = Environment.GetEnvironmentVariable("KAFKA_SERVICE_URI");
            var KAFKA_SERVICE_HOST = Environment.GetEnvironmentVariable("KAFKA_SERVICE_HOST") ?? "localhost";
            var KAFKA_SERVICE_PORT = Environment.GetEnvironmentVariable("KAFKA_SERVICE_PORT") ?? "9092";
            var KAFKA_TOPIC = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test";
            var KAFKA_USER = Environment.GetEnvironmentVariable("KAFKA_USER"); // ?? "user";
            var KAFKA_PASS = Environment.GetEnvironmentVariable("KAFKA_PASS"); // ?? "pass123";
            var KAFKA_MECHANISM = Environment.GetEnvironmentVariable("KAFKA_MECHANISM") ?? "plain";

            _enabled = !string.IsNullOrEmpty(KAFKA_SERVICE_URI) || !string.IsNullOrEmpty(KAFKA_SERVICE_HOST);
            if (!_enabled)
            {
                return;
            }

            _queue = new KafkaMessageQueue();
            _queue.Configure(ConfigParams.FromTuples(
                "topic", KAFKA_TOPIC,
                "connection.protocol", "tcp",
                "connection.uri", KAFKA_SERVICE_URI,
                "connection.host", KAFKA_SERVICE_HOST,
                "connection.port", KAFKA_SERVICE_PORT,
                "credential.username", KAFKA_USER,
                "credential.password", KAFKA_PASS,
                "credential.mechanism", KAFKA_MECHANISM,
                "options.autosubscribe", true
            ));

            _queue.OpenAsync(null).Wait();
            _queue.ClearAsync(null).Wait();

            _fixture = new MessageQueueFixture(_queue);
        }

        public void Dispose()
        {
            if (_queue != null)
            {
                _queue.CloseAsync(null).Wait();
            }
        }

        [Fact]
        public void TestQueueSendReceiveMessage()
        {
            if (_enabled)
            {
                _fixture.TestSendReceiveMessageAsync().Wait();
            }
        }

        [Fact]
        public void TestQueueReceiveSendMessage()
        {
            if (_enabled)
            {
                _fixture.TestReceiveSendMessageAsync().Wait();
            }
        }

        [Fact]
        public void TestQueueSendPeekMessage()
        {
            if (_enabled)
                _fixture.TestSendPeekMessageAsync().Wait();
        }

        [Fact]
        public void TestQueuePeekNoMessage()
        {
            if (_enabled)
                _fixture.TestPeekNoMessageAsync().Wait();
        }

        [Fact]
        public void TestQueueOnMessage()
        {
            if (_enabled)
            {
                _fixture.TestOnMessageAsync().Wait();
            }
        }

    }
}
