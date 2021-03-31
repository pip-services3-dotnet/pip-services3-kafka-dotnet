namespace PipServices3.Kafka.Connect
{
    public interface IKafkaMessageListener
    {
        void OnMessage(KafkaMessage msg);
    }
}
