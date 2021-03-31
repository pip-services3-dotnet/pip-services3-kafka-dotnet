using PipServices3.Kafka.Queues;
using PipServices3.Components.Build;
using PipServices3.Commons.Refer;

namespace PipServices3.Kafka.Build
{
    /// <summary>
    /// Creates KafkaMessageQueue components by their descriptors.
    /// </summary>
    public class DefaultKafkaFactory: Factory
    {
        private static Descriptor KafkaMessageQueueFactoryDescriptor = new Descriptor("pip-services", "queue-factory", "kafka", "*", "1.0");
        private static Descriptor KafkaMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "kafka", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultKafkaFactory()
        {
            RegisterAsType(KafkaMessageQueueFactoryDescriptor, typeof(KafkaMessageQueueFactory));
            Register(KafkaMessageQueueDescriptor, (locator) => {
                Descriptor descriptor = locator as Descriptor;
                var name = descriptor != null ? descriptor.Name : null;
                return new KafkaMessageQueue(name);
            });
        }
    }
}
