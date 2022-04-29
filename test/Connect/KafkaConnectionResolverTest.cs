using System;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using Xunit;

namespace PipServices3.Kafka.Connect
{
    public class KafkaConnectionResolverTest: IDisposable
    {

		public void Dispose()
		{ }

        [Fact]
        public async Task TestSingleConnectionAsync()
        {
			var resolver = new KafkaConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connection.protocol", "tcp",
				"connection.host", "localhost",
				"connection.port", 9092
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("localhost:9092", connection.GetAsString("servers"));
			Assert.Null(connection.GetAsString("username"));
			Assert.Null(connection.GetAsString("password"));
			Assert.Null(connection.GetAsString("mechanism"));
		}

		[Fact]
		public async Task TestSingleConnectionWithAuthAsync()
        {
			var resolver = new KafkaConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connection.protocol", "tcp",
				"connection.host", "localhost",
				"connection.port", 9092,
				"credential.username", "test",
				"credential.password", "pass123",
				"credential.mechanism", "plain"
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("localhost:9092", connection.GetAsString("servers"));
			Assert.Equal("test", connection.GetAsString("username"));
			Assert.Equal("pass123", connection.GetAsString("password"));
			Assert.Equal("plain", connection.GetAsString("mechanism"));
		}

		[Fact]
		public async Task TestClusterConnectionAsync()
        {
			var resolver = new KafkaConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connections.0.protocol", "tcp",
				"connections.0.host", "server1",
				"connections.0.port", 9092,
				"connections.1.protocol", "tcp",
				"connections.1.host", "server2",
				"connections.1.port", 9092,
				"connections.2.protocol", "tcp",
				"connections.2.host", "server3",
				"connections.2.port", 9092
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("server1:9092,server2:9092,server3:9092", connection.GetAsString("servers"));
			Assert.Null(connection.GetAsString("username"));
			Assert.Null(connection.GetAsString("password"));
			Assert.Null(connection.GetAsString("mechanism"));
		}

		[Fact]
		public async Task TestClusterConnectionWithAuthAsync()
		{
			var resolver = new KafkaConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connections.0.protocol", "tcp",
				"connections.0.host", "server1",
				"connections.0.port", 9092,
				"connections.1.protocol", "tcp",
				"connections.1.host", "server2",
				"connections.1.port", 9092,
				"connections.2.protocol", "tcp",
				"connections.2.host", "server3",
				"connections.2.port", 9092,
				"credential.username", "test",
				"credential.password", "pass123",
				"credential.mechanism", "plain"
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("server1:9092,server2:9092,server3:9092", connection.GetAsString("servers"));
			Assert.Equal("test", connection.GetAsString("username"));
			Assert.Equal("pass123", connection.GetAsString("password"));
			Assert.Equal("plain", connection.GetAsString("mechanism"));
		}

		[Fact]
		public async Task TestUriConnectionWithAuthAsync()
		{
			var resolver = new KafkaConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connection.uri", "tcp://test:pass123@server1:9092,server2:9092,server3:9092",
				"credential.mechanism", "plain"
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("server1:9092,server2:9092,server3:9092", connection.GetAsString("servers"));
			Assert.Equal("test", connection.GetAsString("username"));
			Assert.Equal("pass123", connection.GetAsString("password"));
			Assert.Equal("plain", connection.GetAsString("mechanism"));
		}
	}
}
