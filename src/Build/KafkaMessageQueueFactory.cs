using PipServices3.Commons.Refer;
using PipServices3.Messaging.Build;
using PipServices3.Messaging.Queues;
using PipServices3.Kafka.Queues;

namespace PipServices3.Kafka.Build
{
    public class KafkaMessageQueueFactory : MessageQueueFactory
    {
        private static readonly Descriptor KafkaMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "kafka", "*", "1.0");

        public KafkaMessageQueueFactory()
        {
            Register(KafkaMessageQueueDescriptor, (locator) => {
                Descriptor descriptor = locator as Descriptor;
                var name = descriptor != null ? descriptor.Name : null;
                return CreateQueue(name);
            });
        }

        public override IMessageQueue CreateQueue(string name)
        {
            var queue = new KafkaMessageQueue(name);
            if (_config != null)
            {
                queue.Configure(_config);
            }
            if (_references != null)
            {
                queue.SetReferences(_references);
            }
            return queue;
        }
    }
}
