using System;
using System.Text;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Messaging.Queues;
using PipServices3.Kafka.Connect;
using Confluent.Kafka;

namespace PipServices3.Kafka.Queues
{
    /// <summary>
    /// KafkaMessageQueue are message queue that sends and receives messages via Kafka message broker.
    ///
    /// Configuration parameters:
    ///
    /// - topic:                         name of Kafka topic to subscribe
    /// - group_id:                      (optional) consumer group id (default: default)
    /// - from_beginning:                (optional) restarts receiving messages from the beginning (default: false)
    /// - read_partitions:               (optional) number of partitions to be consumed concurrently (default: 1)
    /// - autocommit:                    (optional) turns on/off autocommit (default: true)
    /// - connection(s) :
    ///  - discovery_key:               (optional) a key to retrieve the connection from  IDiscovery
    ///  - host:                        host name or IP address
    ///  - port:                        port number
    ///  - uri:                         resource URI or connection string with all parameters in it
    /// - credential(s) :
    ///  - store_key:                   (optional) a key to retrieve the credentials from  ICredentialStore
    ///  - username:                    user name
    ///  - password:                    user password
    /// - options:
    ///     - listen_connection:    (optional) listening if the connection is alive (default: false) 
    ///     - autosubscribe:        (optional) true to automatically subscribe on option(default: false)
    ///     - acks                  (optional) control the number of required acks: -1 - all, 0 - none, 1 - only leader (default: -1)
    ///     - autocommit_timeout    (optional) number of milliseconds to perform autocommit offsets (default: 1000)
    ///     - connect_timeout:      (optional) number of milliseconds to connect to broker (default: 1000)
    ///     - max_retries:          (optional) maximum retry attempts (default: 5)
    ///     - retry_timeout:        (optional) number of milliseconds to wait on each reconnection attempt (default: 30000)
    ///     - request_timeout:      (optional) number of milliseconds to wait on broker request (default: 30000)
    ///     - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 30000)
    ///
    ///
    /// References:
    ///
    /// - *:logger:*:*:1.0             (optional) ILogger components to pass log messages
    /// - *:counters:*:*:1.0           (optional) ICounters components to pass collected measurements
    /// - *:discovery:*:*:1.0          (optional) IDiscovery services to resolve connections
    /// - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
    /// - *:connection:kafka:*:1.0      (optional) Shared connection to Kafka service
    ///
    /// See MessageQueue
    /// See MessagingCapabilities
    ///
    /// Example:
    ///
    ///    var queue = new KafkaMessageQueue("myqueue");
    ///    queue.Configure(ConfigParams.FromTuples(
    ///      "subject", "mytopic",
    ///      "connection.protocol", "tcp"
    ///      "connection.host", "localhost"
    ///      "connection.port", 9092
    ///    ));
    ///
    ///    await queue.OpenAsync("123");
    ///
    ///    await queue.SendAsync("123", new MessageEnvelope("", "mymessage", "ABC"));
    ///
    ///    var message = await queue.ReceiveAsync("123");
    ///    if (message != null) {
    ///		...
    ///		await queue.CompleteAsync("123", message);
    ///    }
    /// </summary>
    public class KafkaMessageQueue : CachedMessageQueue, IKafkaMessageListener
    {
        private ConfigParams _config;
        private IReferences _references;
        private bool _opened;
        private bool _localConnection;
        private DependencyResolver _dependencyResolver = new DependencyResolver();
        private KafkaConnection _connection;
        private bool _subscribed;

        private string _topic;
        private string _groupId = "default";
        private bool _fromBeginning;
        private int _readPartitions;
        private bool _autocommit;
        private int _autocommitTimeout;

        private bool _listenConnection = false;

        /// <summary>
        /// The Kafka connection listener component.
        /// </summary>
        protected KafkaConnectionListener _connectionListener;

        public KafkaMessageQueue(string name = null)
            : base(name, new MessagingCapabilities(false, true, true, true, true, false, false, false, true))
        {
            _dependencyResolver.Put("connection", new Descriptor("pip-services", "connection", "kafka", "*", "1.0"));
        }

        /// <summary>
        /// Configure are configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">Configuration parameters to be set</param>
        public override void Configure(ConfigParams config)
        {
            base.Configure(config);
            _dependencyResolver.Configure(config);

            _config = config;

            _topic = config.GetAsStringWithDefault("topic", _topic);
            _groupId = config.GetAsStringWithDefault("group_id", _groupId);
            _fromBeginning = config.GetAsBooleanWithDefault("from_beginning", _fromBeginning);
            _readPartitions = config.GetAsIntegerWithDefault("read_partitions", _readPartitions);
            _autocommit = config.GetAsBooleanWithDefault("autocommit", _autocommit);
            _autocommitTimeout = config.GetAsIntegerWithDefault("options.autocommit_timeout", _autocommitTimeout);
            _listenConnection = config.GetAsBooleanWithDefault("options.listen_connection", _listenConnection);
        }

        /// <summary>
        /// SetReferences are sets references to dependent components.
        /// </summary>
        /// <param name="references">References to be set</param>
        public override void SetReferences(IReferences references)
        {
            base.SetReferences(references);

            _references = references;

            _dependencyResolver.SetReferences(references);
            // Get connection
            _connection = _dependencyResolver.GetOneOptional<KafkaConnection>("connection");

            // Or create a local one
            if (_connection == null)
            {
                _connection = CreateConnection();

                if (_listenConnection)
                    _connectionListener = CreateConnectionListener();

                _localConnection = true;
            }
            else
            {
                _localConnection = false;
            }
        }

        private KafkaConnection CreateConnection()
        {
            var connection = new KafkaConnection();
            var reference = new Reference(new Descriptor("pip-services", "connection", "kafka", "*", "1.0"), connection);

            if (_config != null)
            {
                connection.Configure(_config);
            }

            if (_references != null)
            {
                connection.SetReferences(_references);
                _references.Put(reference.GetLocator(), reference.GetComponent());
            }
            else
            {
                _references = References.FromTuples(reference.GetLocator(), reference.GetComponent());
            }

            return connection;
        }

        private KafkaConnectionListener CreateConnectionListener()
        {
            var connectionListener = new KafkaConnectionListener();
            var reference = new Reference(new Descriptor("pip-services", "connection-listener", "kafka", "*", "1.0"), connectionListener);

            if (_config != null)
                connectionListener.Configure(_config);

            if (_references != null)
            {
                connectionListener.SetReferences(_references);
                _references.Put(reference.GetLocator(), reference.GetComponent());
            }
            else
            {
                _references = References.FromTuples(reference.GetLocator(), reference.GetComponent());
            }

            return connectionListener;
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public override bool IsOpen()
        {
            return _opened;
        }

        /// <summary>
        /// Opens the component with given connection and credential parameters.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public async override Task OpenAsync(string correlationId)
        {
            if (IsOpen())
            {
                return;
            }

            if (_connection == null)
            {
                _connection = CreateConnection();
                if (_listenConnection)
                    _connectionListener = CreateConnectionListener();

                _localConnection = true;
            }

            if (_localConnection)
            {
                await _connection.OpenAsync(correlationId);

                if (_listenConnection)
                    await _connectionListener.OpenAsync(correlationId);
            }

            if (!_connection.IsOpen())
            {
                throw new InvalidStateException(correlationId, "CONNECT_FAILED", "Kafka connection is not opened");
            }

            await base.OpenAsync(correlationId);

            _opened = true;
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public override async Task CloseAsync(string correlationId)
        {
            if (!IsOpen())
            {
                return;
            }

            if (_connection == null)
            {
                throw new InvalidStateException(correlationId, "NO_CONNECTION", "Kafka connection is missing");
            }

            if (_localConnection)
            {
                if (_listenConnection)
                    await _connectionListener.CloseAsync(correlationId);

                await _connection.CloseAsync(correlationId);
            }

            _opened = false;

            await base.CloseAsync(correlationId);
        }

        private string GetTopic()
        {
            return !string.IsNullOrEmpty(_topic) ? _topic : Name;
        }

        protected override async Task SubscribeAsync(string correlationId)
        {
            // Check if already were subscribed
            if (_subscribed)
            {
                return;
            }

            // Subscribe to the topic
            var topic = GetTopic();
            var config = new ConsumerConfig
            {
                EnableAutoCommit = _autocommit,
                AutoCommitIntervalMs = _autocommitTimeout
            };

            await _connection.SubscribeAsync(topic, _groupId, config, this);

            _subscribed = true;
        }

        protected override async Task UnsubscribeAsync(string correlationId)
        {
            // Check if already were unsubscribed
            if (!_subscribed)
            {
                return;
            }

            // Unsubscribe from the topic
            var topic = GetTopic();
            await _connection.UnsubscribeAsync(topic, _groupId, this);

            _subscribed = false;
        }

        private Message<byte[], byte[]> FromMessage(MessageEnvelope message)
        {
            var data = message.Message;

            var headers = new Headers();
            if (message.CorrelationId != null)
            {
                headers.Add("correlation_id", Encoding.UTF8.GetBytes(message.CorrelationId));
            }
            if (message.MessageType != null)
            {
                headers.Add("message_type", Encoding.UTF8.GetBytes(message.MessageType));
            }
            var sentTime = StringConverter.ToNullableString(DateTime.UtcNow);
            headers.Add("sent_time", Encoding.UTF8.GetBytes(sentTime));

            var msg = new Message<byte[], byte[]>();
            msg.Headers = headers;

            if (message.MessageId != null)
            {
                msg.Key = Encoding.UTF8.GetBytes(message.MessageId);
            }
            msg.Value = message.Message;

            return msg;
        }

        private MessageEnvelope ToMessage(KafkaMessage msg)
        {
            if (msg == null || msg.Message == null)
            {
                return null;
            }

            var correlationId = GetHeaderByKey(msg.Message.Headers, "correlation_id");
            var messageType = GetHeaderByKey(msg.Message.Headers, "message_type");

            MessageEnvelope message = new MessageEnvelope(correlationId, messageType, msg.Message.Value);

            if (msg.Message.Key != null)
            {
                message.MessageId = Encoding.UTF8.GetString(msg.Message.Key);
            }

            var sentTime = GetHeaderByKey(msg.Message.Headers, "sent_time");
            message.SentTime = DateTimeConverter.ToDateTime(sentTime);

            message.Reference = msg;

            return message;
        }

        private string GetHeaderByKey(Headers headers, string key)
        {
            if (headers == null)
            {
                return null;
            }

            byte[] value = null;
            headers.TryGetLastBytes(key, out value);

            if (value != null)
            {
                return Encoding.UTF8.GetString(value);
            }

            return null;
        }

        public void OnMessage(KafkaMessage msg)
        {
            // Deserialize message
            var message = ToMessage(msg);
            if (message == null)
            {
                _logger.Error(null, null, "Failed to read received message");
                return;
            }

            _counters.IncrementOne("queue." + Name + ".received_messages");
            _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, Name);

            if (_receiver != null)
            {
                SendMessageToReceiver(_receiver, message);
            }
            else
            {
                lock (_lock)
                {
                    _messages.Enqueue(message);
                }

                _receiveEvent.Set();
            }
        }

        private void SendMessageToReceiver(IMessageReceiver receiver, MessageEnvelope message)
        {
            var correlationId = message?.CorrelationId;

            try
            {
                receiver.ReceiveMessageAsync(message, this).Wait();
            }
            catch (Exception ex)
            {
                _logger.Error(correlationId, ex, "Failed to process the message");
            }
        }

        /// <summary>
        /// Sends a message into the queue.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="message">a message envelop to be sent.</param>
        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpen(correlationId);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via %s", message, Name);

            var msg = FromMessage(message);

            var topic = !string.IsNullOrEmpty(Name) ? Name : _topic;

            await _connection.PublishAsync(topic, msg);
        }

        /// <summary>
        /// Renews a lock on a message that makes it invisible from other receivers in
        /// the queue.This method is usually used to extend the message processing time.
        /// </summary>
        /// <param name="message">a message to extend its lock.</param>
        /// <param name="lockTimeout">a locking timeout in milliseconds.</param>
        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            // Not supported
            await Task.Delay(0);
        }

        /// <summary>
        /// Returns message into the queue and makes it available for all subscribers to
        /// receive it again.This method is usually used to return a message which could
        /// not be processed at the moment to repeat the attempt.Messages that cause
        /// unrecoverable errors shall be removed permanently or/and send to dead letter queue.
        /// </summary>
        /// <param name="message">a message to return.</param>
        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpen(null);

            var msg = message.Reference as KafkaMessage;
            if (_autocommit || msg == null)
            {
                return;
            }

            // Rollback to the message offset so it will come back again
            msg.Consumer.StoreOffset(msg.Result);
            message.Reference = null;

            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue. This method is usually used to
        /// remove the message after successful processing.
        /// </summary>
        /// <param name="message">a message to remove.</param>
        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpen(null);

            var msg = message.Reference as KafkaMessage;
            if (_autocommit || msg == null)
            {
                return;
            }

            // Commit the message offset so it won't come back
            msg.Consumer.Commit(msg.Result);
            message.Reference = null;

            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue and sends it to dead letter queue.
        /// </summary>
        /// <param name="message">a message to be removed.</param>
        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            // Not supported
            await Task.Delay(0);
        }


    }
}
