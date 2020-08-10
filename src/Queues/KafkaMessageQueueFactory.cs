using PipServices3.Components.Build;
using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;

namespace PipServices3.Kafka.Queues
{
    public class KafkaMessageQueueFactory : Factory, IConfigurable
    {
        public static readonly Descriptor Descriptor = new Descriptor("pip-services3-kafka", "factory", "message-queue", "kafka", "1.0");
        public static readonly Descriptor MemoryQueueDescriptor = new Descriptor("pip-services3-kafka", "message-queue", "kafka", "*", "*");

        private ConfigParams _config;

        public KafkaMessageQueueFactory()
        {
            Register(MemoryQueueDescriptor, (locator) => {
                Descriptor descriptor = (Descriptor)locator;
                var queue = new KafkaMessageQueue(descriptor.Name);
                if (_config != null)
                    queue.Configure(_config);
                return queue;
            });
        }

        public void Configure(ConfigParams config)
        {
            _config = config;
        }
    }
}
