using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using Xunit;

namespace PipServices3.Kafka.Connect
{
    public class KafkaConnectionTest : IDisposable
    {
        private bool _enabled;
        private KafkaConnection _connection;

        public KafkaConnectionTest()
        {
            var KAFKA_SERVICE_URI = Environment.GetEnvironmentVariable("KAFKA_SERVICE_URI");
            var KAFKA_SERVICE_HOST = Environment.GetEnvironmentVariable("KAFKA_SERVICE_HOST") ?? "localhost";
            var KAFKA_SERVICE_PORT = Environment.GetEnvironmentVariable("KAFKA_SERVICE_PORT") ?? "9092";
            var KAFKA_USER = Environment.GetEnvironmentVariable("KAFKA_USER"); // ?? "user";
            var KAFKA_PASS = Environment.GetEnvironmentVariable("KAFKA_PASS"); // ?? "pass123";
            var KAFKA_MECHANISM = Environment.GetEnvironmentVariable("KAFKA_MECHANISM") ?? "plain";

            _enabled = !string.IsNullOrEmpty(KAFKA_SERVICE_URI) || !string.IsNullOrEmpty(KAFKA_SERVICE_HOST);
            if (!_enabled)
            {
                return;
            }

            _connection = new KafkaConnection();
            _connection.Configure(ConfigParams.FromTuples(
                "connection.uri", KAFKA_SERVICE_URI,
                "connection.protocol", "tcp",
                "connection.host", KAFKA_SERVICE_HOST,
                "connection.port", KAFKA_SERVICE_PORT,
                "credential.username", KAFKA_USER,
                "credential.password", KAFKA_PASS,
                "credential.mechanism", KAFKA_MECHANISM
            ));
        }

        public void Dispose()
        {
        }

        [Fact]
        public async Task TestConnectionOpenCloseAsync()
        {
            if (_enabled)
            {
                await _connection.OpenAsync(null);

                Assert.True(_connection.IsOpen());
                Assert.NotNull(_connection.GetConnection());

                await _connection.CloseAsync(null);

                Assert.False(_connection.IsOpen());
                Assert.Null(_connection.GetConnection());
            }
        }

        [Fact]
        public async Task TestConnectionGetTopicsAsync()
        {
            if (_enabled)
            {
                await _connection.OpenAsync(null);
                Assert.True(_connection.IsOpen());
                Assert.NotNull(_connection.GetConnection());

                var topics = await _connection.ReadQueueNamesAsync();
                Assert.IsType<List<string>>(topics);

                await _connection.CloseAsync(null);
                Assert.False(_connection.IsOpen());
                Assert.Null(_connection.GetConnection());
            }
        }

    }
}
