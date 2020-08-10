using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Errors;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;
using PipServices3.Messaging.Queues;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using PipServices3.Commons.Data;

namespace PipServices3.Kafka.Queues
{
    /// <summary>
    /// Message queue that sends and receives messages via Apache Kafka message broker.
    /// 
    /// Apache Kafka is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.
    /// 
    /// ### Configuration parameters ###
    /// 
    /// - topic.name:                  name of Kafka topic to subscribe
    /// 
    /// connection(s):
    /// - discovery_key:               (optional) a key to retrieve the connection from <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_connect_1_1_i_discovery.html">IDiscovery</a>
    /// - host:                        host name or IP address
    /// - port:                        port number
    /// - uri:                         resource URI or connection string with all parameters in it
    /// 
    /// credential(s):
    /// - store_key:                   (optional) a key to retrieve the credentials from <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_auth_1_1_i_credential_store.html">ICredentialStore</a>
    /// - username:                    user name
    /// - password:                    user password
    /// 
    /// ### References ###
    /// 
    /// - *:logger:*:*:1.0             (optional) <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_log_1_1_i_logger.html">ILogger</a> components to pass log messages
    /// - *:counters:*:*:1.0           (optional) <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_count_1_1_i_counters.html">ICounters</a> components to pass collected measurements
    /// - *:discovery:*:*:1.0          (optional) <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_connect_1_1_i_discovery.html">IDiscovery</a> services to resolve connections
    /// - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
    /// </summary>
    /// <example>
    /// <code>
    /// var queue = new KafkaMessageQueue("myqueue");
    /// queue.configure(ConfigParams.FromTuples(
    /// "topic.name", "mytopic",
    /// "connection.host", "localhost"
    /// "connection.port", 9092 ));
    /// queue.Open("123");
    /// 
    /// queue.Send("123", new MessageEnvelop(null, "mymessage", "ABC"));
    /// queue.Receive("123", 0);
    /// queue.Complete("123", message);
    /// </code>
    /// </example>
    public class KafkaMessageQueue : MessageQueue
    {
        private const string MSG_HEADER_TYPE = "type";
        private const string MSG_HEADER_CORRELATIONID = "correlationId";

        private readonly AdminClientConfig _adminClientConfig;
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        private readonly TopicSpecification _topicSpec;

        private IProducer<string, string> _producer;
        private IConsumer<string, string> _consumer;
        private IAdminClient _adminClient;

        private CancellationTokenSource _cancel = new CancellationTokenSource();

        private readonly int DefaultPort = 9092;

        public override long? MessageCount
        {
            get
            {
                CheckOpened(null);
                return CalculateMessageCount();
            }
        }

        /// <summary>
        /// Creates a new instance of the message queue.
        /// </summary>
        /// <param name="name">(optional) a queue name.</param>
        public KafkaMessageQueue(string name = null)
        {
            _adminClientConfig = new AdminClientConfig();
            _producerConfig = new ProducerConfig();
            _consumerConfig = new ConsumerConfig();
            _topicSpec = new TopicSpecification();

            Name = name ?? string.Format("queue-{0}", IdGenerator.NextLong());

            Capabilities = new MessagingCapabilities(false, true, true, false, false, false, true, false, false);
        }

        public KafkaMessageQueue(string name, ConfigParams config)
            : this(name)
        {
            if (config != null) Configure(config);
        }

        /// <summary>
        /// Configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">configuration parameters to be set.</param>
        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            _topicSpec.Name = config.GetAsStringWithDefault("topic.name", Name);
            _topicSpec.NumPartitions = config.GetAsIntegerWithDefault("topic.num_partitions", 1);
            _topicSpec.ReplicationFactor = Convert.ToInt16(config.GetAsIntegerWithDefault("topic.replication_factor", -1));
            
            _consumerConfig.GroupId = config.GetAsStringWithDefault("consumer.group_id", "custom-group");
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public override bool IsOpen()
        {
            return _producer != null && _consumer != null && _adminClient != null;
        }

        /// <summary>
        /// Opens the component with given connection and credential parameters.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="connection">connection parameters</param>
        /// <param name="credential">credential parameters</param>
        public async override Task OpenAsync(string correlationId, ConnectionParams connection, CredentialParams credential)
        {
            string uri = !string.IsNullOrEmpty(connection.Uri)
                ? connection.Uri
                : string.Format("{0}:{1}", connection.Host, connection.Port != 0 ? connection.Port : DefaultPort);

            SecurityProtocol securityProtocol = MapProtocol(connection.Protocol);

            SetupConfig(_adminClientConfig, uri, securityProtocol, credential);
            SetupConfig(_producerConfig, uri, securityProtocol, credential);
            SetupConfig(_consumerConfig, uri, securityProtocol, credential);

            _adminClient = CreateAdminClient(correlationId, _adminClientConfig);

            await CreateTopicAsync(correlationId);

            _producer = CreateProducer(correlationId, _producerConfig);
            _consumer = CreateConsumer(correlationId, _topicSpec.Name, _consumerConfig);
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public override async Task CloseAsync(string correlationId)
        {
            _cancel.Cancel();

            CloseProducer();
            CloseConsumer();
            CloseAdminClient();

            _logger.Trace(correlationId, "Closed queue {0}", this);

            await Task.Delay(0);
        }

        /// <summary>
        /// Sends a message into the queue.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="message">a message envelop to be sent.</param>
        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            
            var envelope = ToEnvelope(message);
            await _producer.ProduceAsync(_topicSpec.Name, envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        /// <summary>
        /// Receives an incoming message and removes it from the queue.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="waitTimeout">a timeout in milliseconds to wait for a message to come.</param>
        /// <returns>a message</returns>
        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CancellationTokenSource cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(waitTimeout));
            ConsumeResult<string, string> consumeResult = _consumer.Consume(cancel.Token);

            var message = ToMessage(consumeResult?.Message);

            if (message != null)
            {
                _counters.IncrementOne("queue." + Name + ".received_messages");
                _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
            }

            return await Task.FromResult(message);
        }

        /// <summary>
        /// Listens for incoming messages and blocks the current thread until queue is closed.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="callback"></param>
        /// <returns></returns>
        public override async Task ListenAsync(string correlationId, Func<MessageEnvelope, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Debug(correlationId, "Started listening messages at {0}", this);

            // Create new cancelation token
            _cancel = new CancellationTokenSource();

            while (!_cancel.IsCancellationRequested)
            {
                var envelope = _consumer.Consume(_cancel.Token);

                if (envelope != null && !_cancel.IsCancellationRequested)
                {
                    var message = ToMessage(envelope.Message);

                    _counters.IncrementOne("queue." + Name + ".received_messages");
                    _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);

                    try
                    {
                        await callback(message, this);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(correlationId, ex, "Failed to process the message");
                        //throw ex;
                    }
                }
            }
        }

        /// <summary>
        /// Ends listening for incoming messages.
        /// When this method is call listen unblocks the thread and execution continues.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public override void EndListen(string correlationId)
        {
            _cancel.Cancel();
        }

        /// <summary>
        /// Returnes message into the queue and makes it available for all subscribers to receive it again.
        /// This method is usually used to return a message which could not be processed at the moment
        /// to repeat the attempt.Messages that cause unrecoverable errors shall be removed permanently
        /// or/and send to dead letter queue.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to return.</param>
        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            // Make the message immediately visible
            var envelope = message.Reference as Message<string, string>;
            if (envelope != null)
            {
                await _producer.ProduceAsync(_topicSpec.Name, envelope);

                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }

            await Task.Delay(0);
        }

        /// <summary>
        /// Clears component state.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <returns></returns>
        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            await Task.Delay(0);

            _logger.Trace(null, "Cleared queue {0}", this);
        }

        #region Operations that are not supported
        /// <summary>
        /// Peeks a single incoming message from the queue without removing it.
        /// If there are no messages available in the queue it returns null.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <returns>a message</returns>
        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);

            // Operation is not supported

            return await Task.FromResult<MessageEnvelope>(null);
        }

        /// <summary>
        /// Peeks multiple incoming messages from the queue without removing them.
        /// If there are no messages available in the queue it returns an empty list.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="messageCount">a maximum number of messages to peek.</param>
        /// <returns>a list with messages</returns>
        public override async Task<List<MessageEnvelope>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);

            // Operation is not supported

            return await Task.FromResult<List<MessageEnvelope>>(null);
        }

        /// <summary>
        /// Renews a lock on a message that makes it invisible from other receivers in the queue.
        /// This method is usually used to extend the message processing time.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to extend its lock.</param>
        /// <param name="lockTimeout">a locking timeout in milliseconds.</param>
        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue and sends it to dead letter queue.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to be removed.</param>
        /// <returns></returns>
        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue.
        /// This method is usually used to remove the message after successful processing.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to remove.</param>
        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }
        #endregion

        #region Helper methods
        private long CalculateMessageCount()
        {
            long total = 0;

            foreach (var partition in _consumer.Assignment)
            {
                var offsets = _consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5));
                total += offsets.High.Value - offsets.Low.Value;
            }

            return total;
        }

        private bool TopicExist(string name)
        {
            Metadata metadata = _adminClient.GetMetadata(name, TimeSpan.FromSeconds(5));
            return metadata != null && metadata.Topics.Count > 0;
        }

        private void SetupConfig(ClientConfig clientConfig, string uri, SecurityProtocol securityProtocol, CredentialParams credential)
        {
            clientConfig.BootstrapServers = uri;

            if (credential != null && !string.IsNullOrEmpty(credential.Username))
            {
                clientConfig.SecurityProtocol = securityProtocol;
                clientConfig.SaslUsername = credential.Username;
                clientConfig.SaslPassword = credential.Password;
            }
        }

        private SecurityProtocol MapProtocol(string protocol)
        {
            switch (protocol.ToLower())
            {
                case "ssl": return SecurityProtocol.Ssl;
                case "saslssl": return SecurityProtocol.SaslSsl;
                case "saslplaintext": return SecurityProtocol.SaslPlaintext;
                default: return SecurityProtocol.Plaintext;
            }
        }

        private async Task CreateTopicAsync(string correlationId)
        {
            try
            {
                await _adminClient.CreateTopicsAsync(new[] { _topicSpec });
            }
            catch (CreateTopicsException ex)
            {
                if ((ex.Error.Code == ErrorCode.Local_Partial && ex.Error.Reason.Contains("already exists")) ||
                    ex.Error.Code == ErrorCode.TopicAlreadyExists)
                    return;

                _logger.Error(correlationId, ex, "Failed to create Kafka topic with name {0}", _topicSpec.Name);
                //throw ex;
            }
        }

        private IAdminClient CreateAdminClient(string correlationId, AdminClientConfig config)
        {
            try
            {
                return new AdminClientBuilder(config).Build();
            }
            catch (Exception ex)
            {
                throw new ConnectionException(
                    correlationId,
                    "CANNOT_CONNECT",
                    "Cannot connect admin to Kafka at " + config.BootstrapServers
                ).Wrap(ex);
            }
        }

        private IConsumer<string, string> CreateConsumer(string correlationId, string topic, ConsumerConfig config)
        {
            try
            {
                IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(topic);
                

                return consumer;
            }
            catch (Exception ex)
            {
                throw new ConnectionException(
                    correlationId,
                    "CANNOT_CONNECT",
                    "Cannot connect consumer to Kafka at " + config.BootstrapServers
                ).Wrap(ex);
            }
        }

        private IProducer<string, string> CreateProducer(string correlationId, ProducerConfig config)
        {
            try
            {
                return new ProducerBuilder<string, string>(config).Build();
            }
            catch (Exception ex)
            {
                throw new ConnectionException(
                    correlationId,
                    "CANNOT_CONNECT",
                    "Cannot connect producer to Kafka at " + config.BootstrapServers
                ).Wrap(ex);
            }
        }

        private void CloseAdminClient()
        {
            var adminClient = _adminClient;
            if (adminClient != null)
            {
                adminClient.Dispose();
            }

            _adminClient = null;
        }

        private void CloseConsumer()
        {
            var consumer = _consumer;
            if (consumer != null)
            {
                consumer.Close();
                consumer.Dispose();
            }

            _consumer = null;
        }

        private void CloseProducer()
        {
            var producer = _producer;
            if (producer != null)
            {
                producer.Dispose();
            }

            _producer = null;
        }

        private async Task ChangeRetentionAsync(string name, TimeSpan retention, int bytes)
        {
            var configResource = new ConfigResource { Name = name, Type = ResourceType.Topic };
            var toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    {   configResource,
                        new List<ConfigEntry> {
                            new ConfigEntry { Name = "retention.ms", Value = Convert.ToInt32(retention.TotalMilliseconds).ToString() },
                            new ConfigEntry { Name = "retention.bytes", Value = bytes.ToString() }
                        }
                    }
                };

            try
            {
                await _adminClient.AlterConfigsAsync(toUpdate);
            }
            catch
            {
            }
        }

        private void CheckOpened(string correlationId)
        {
            if (!IsOpen())
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        private static MessageEnvelope ToMessage(Message<string, string> envelope)
        {
            if (envelope == null) return null;

            string messageType = GetHeaderByKey(envelope.Headers, MSG_HEADER_TYPE);
            string correlationId = GetHeaderByKey(envelope.Headers, MSG_HEADER_CORRELATIONID);

            MessageEnvelope message = new MessageEnvelope
            {
                MessageId = envelope.Key,
                MessageType = messageType,
                CorrelationId = correlationId,
                Message = envelope.Value,
                SentTime = envelope.Timestamp.UtcDateTime,
                Reference = envelope
            };

            return message;
        }

        private static Message<string, string> ToEnvelope(MessageEnvelope envelope)
        {
            if (envelope == null) return null;

            Headers headers = new Headers
            {
                { MSG_HEADER_TYPE, Encoding.UTF8.GetBytes(envelope.MessageType) },
                { MSG_HEADER_CORRELATIONID, Encoding.UTF8.GetBytes(envelope.CorrelationId) }
            };

            Message<string, string> message = new Message<string, string>
            {
                Key = envelope.MessageId,
                Value = envelope.Message,
                Headers = headers,
                Timestamp = new Timestamp(envelope.SentTime)
            };

            return message;
        }

        private static string GetHeaderByKey(Headers headers, string key)
        {
            if (headers != null)
            {
                try
                {
                    byte[] bytes = headers.GetLastBytes(key);
                    return Encoding.UTF8.GetString(bytes);
                }
                catch (KeyNotFoundException)
                {
                }
            }

            return null;
        }
        #endregion
    }
}
