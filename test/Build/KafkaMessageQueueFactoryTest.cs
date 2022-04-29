using System;
using PipServices3.Commons.Refer;
using PipServices3.Kafka.Queues;
using Xunit;

namespace PipServices3.Kafka.Build
{
    public class KafkaMessageQueueFactoryTest : IDisposable
	{

		public void Dispose()
		{ }

		[Fact]
		public void TestFactoryCreateMessageQueue()
		{
			var factory = new KafkaMessageQueueFactory();
			var descriptor = new Descriptor("pip-services", "message-queue", "kafka", "test", "1.0");

			var canResult = factory.CanCreate(descriptor);
			Assert.NotNull(canResult);

			var queue = factory.Create(descriptor) as KafkaMessageQueue;
			Assert.NotNull(queue);
			Assert.Equal("test", queue.Name);
		}

	}
}
