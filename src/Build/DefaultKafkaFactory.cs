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
        public static Descriptor Descriptor = new Descriptor("pip-services", "factory", "kafka", "default", "1.0");
        public static Descriptor Descriptor3 = new Descriptor("pip-services3", "factory", "kafka", "default", "1.0");
        public static Descriptor KafkaMessageQueueFactoryDescriptor = new Descriptor("pip-services", "factory", "message-queue", "kafka", "1.0");
        public static Descriptor KafkaMessageQueueFactory3Descriptor = new Descriptor("pip-services3", "factory", "message-queue", "kafka", "1.0");
        public static Descriptor KafkaMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "kafka", "*", "1.0");
        public static Descriptor KafkaMessageQueue3Descriptor = new Descriptor("pip-services3", "message-queue", "kafka", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultKafkaFactory()
        {
            RegisterAsType(KafkaMessageQueueFactoryDescriptor, typeof(KafkaMessageQueueFactory));
            RegisterAsType(KafkaMessageQueueFactory3Descriptor, typeof(KafkaMessageQueueFactory));
            RegisterAsType(KafkaMessageQueueDescriptor, typeof(KafkaMessageQueue));
            RegisterAsType(KafkaMessageQueue3Descriptor, typeof(KafkaMessageQueue));
        }
    }
}
