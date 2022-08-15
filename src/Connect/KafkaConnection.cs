using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using PipServices3.Commons.Config;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Run;
using PipServices3.Components.Log;
using PipServices3.Messaging.Connect;

namespace PipServices3.Kafka.Connect
{
    /// <summary>
    ///  Kafka connection using plain driver.
    ///  By defining a connection and sharing it through multiple message queues
    ///  you can reduce number of used connections.

    ///  ### Configuration parameters ###
    ///    - client_id:               (optional) name of the client id
    ///    - connection(s):
    ///      - discovery_key:               (optional) a key to retrieve the connection from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]]
    ///      - host:                        host name or IP address
    ///      - port:                        port number
    ///      - uri:                         resource URI or connection string with all parameters in it
    ///    - credential(s):
    ///      - store_key:                   (optional) a key to retrieve the credentials from[[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/auth.icredentialstore.html ICredentialStore]]
    ///      - username:                    user name
    ///      - password:                    user password
    ///    - options:
    ///      - log_level:            (optional) log level 0 - None, 1 - Error, 2 - Warn, 3 - Info, 4 - Debug (default: 1)
    ///      - acks                  (optional) control the number of required acks: -1 - all, 0 - none, 1 - only leader (default: -1)
    ///      - connect_timeout:      (optional) number of milliseconds to connect to broker (default: 1000)
    ///      - max_retries:          (optional) maximum retry attempts (default: 5)
    ///      - retry_timeout:        (optional) number of milliseconds to wait on each reconnection attempt (default: 30000)
    ///      - request_timeout:      (optional) number of milliseconds to wait on broker request (default: 30000)
    ///      - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 30000)
    ///  
    ///  ### References ###
    ///   - \*:logger:\*:\*:1.0(optional) ILogger components to pass log messages
    ///   - \*:discovery:\*:\*:1.0(optional) IDiscovery services
    ///   - \*:credential - store:\*:\*:1.0(optional) Credential stores to resolve credentials
    /// </summary>
    public class KafkaConnection: IMessageQueueConnection, IConfigurable, IReferenceable, IOpenable
    {
        private static ConfigParams _defaultConfig = ConfigParams.FromTuples(
            // "client_id", nil,
            "options.log_level", 1,
            "options.connect_timeout", 1000,
            "options.retry_timeout", 30000,
            "options.max_retries", 5,
            "options.request_timeout", 30000,
            "options.flush_timeout", 30000
        );
        protected CompositeLogger _logger = new CompositeLogger();
        protected KafkaConnectionResolver _connectionResolver = new KafkaConnectionResolver();
        protected ConfigParams _options = new ConfigParams();

        // Kafka connection object
        protected IProducer<byte[], byte[]> _connection;

        // Kafka admin client object
        protected IAdminClient _adminClient;

        // Topic subscriptions
        protected List<KafkaSubscription> _subscriptions = new List<KafkaSubscription>();
        protected object _lock = new object();

        private List<bool> _autocommitSubscriptions = new List<bool>();

        // Connection options
        private string _clientId;
        private int _logLevel = 1;
        private int _acks = 0;
        private int _connectTimeout = 1000;
        private int _maxRetries = 5;
        private int _retryTimeout = 30000;
        private int _requestTimeout = 30000;
        private int _flushTimeout = 30000;

        public KafkaConnection()
        {
            _clientId = Environment.MachineName;
        }

        /// <summary>
        /// Configure are configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">Configuration parameters to be set</param>
        public void Configure(ConfigParams config)
        {
            config = config.SetDefaults(_defaultConfig);
            _connectionResolver.Configure(config);
            _logger.Configure(config);

            _options = _options.Override(config.GetSection("options"));

            _clientId = config.GetAsStringWithDefault("client_id", _clientId);
            _acks = config.GetAsIntegerWithDefault("options.acks", _acks);
            _logLevel = config.GetAsIntegerWithDefault("options.log_level", _logLevel);
            _connectTimeout = config.GetAsIntegerWithDefault("options.retry_connect", _connectTimeout);
            _maxRetries = config.GetAsIntegerWithDefault("options.max_retries", _maxRetries);
            _retryTimeout = config.GetAsIntegerWithDefault("options.retry_timeout", _retryTimeout);
            _requestTimeout = config.GetAsIntegerWithDefault("options.request_timeout", _requestTimeout);
            _flushTimeout = config.GetAsIntegerWithDefault("options.flush_timeout", _flushTimeout);
        }

        /// <summary>
        /// SetReferences are sets references to dependent components.
        /// </summary>
        /// <param name="references">References to be set</param>
        public void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _logger.SetReferences(references);
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public bool IsOpen()
        {
            return _connection != null;
        }

        /// <summary>
        /// Opens the component.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public async Task OpenAsync(string correlationId)
        {
            if (IsOpen())
                return;

            var options = await _connectionResolver.ResolveAsync(correlationId);
            var uri = options.GetAsString("servers");

            var opts = new ProducerConfig
            {
                ClientId = _clientId,
                BootstrapServers = uri,
                RequestTimeoutMs = _requestTimeout,
                Acks = (Acks)_acks,
            };

            var username = options.GetAsString("username");
            if (!string.IsNullOrEmpty(username))
            {
                opts.SaslUsername = username;
                var password = options.GetAsString("password");
                opts.SaslPassword = password;

                var mechanism = options.GetAsString("mechanism");
                switch (mechanism)
                {
                    case "scram-sha-256":
                        opts.SaslMechanism = SaslMechanism.ScramSha256;
                        break;
                    case "scram-sha-512":
                        opts.SaslMechanism = SaslMechanism.ScramSha512;
                        break;
                    default:
                        opts.SaslMechanism = SaslMechanism.Plain;
                        break;
                }
            }

            try
            {
                var producer = new ProducerBuilder<byte[], byte[]>(opts).Build();
                _connection = producer;
            }
            catch (Exception ex)
            {
                _logger.Error(correlationId, ex, "Failed to connect to Kafka broker at " + uri);
                throw ex;
            }

            _logger.Debug(correlationId, "Connected to Kafka broker at " + uri);
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public async Task CloseAsync(string correlationId)
        {
            if (!IsOpen())
                return;

            try
            {
                _connection.Flush(TimeSpan.FromMilliseconds(_flushTimeout));

                // Close all subscriptions
                foreach (var subscription in _subscriptions)
                {
                    try
                    {
                        subscription.Token.Cancel();
                        subscription.Handler.Unsubscribe();
                        subscription.Handler.Close();
                    }
                    catch
                    {
                        // Ignore errors
                    }
                }
            }
            finally
            {
                _connection = null;
                _subscriptions.Clear();
                _adminClient = null;
                _autocommitSubscriptions.Clear();

                _logger.Debug(correlationId, "Disconnected from Kafka broker");
            }

            await Task.Delay(0);
        }

        public IProducer<byte[], byte[]> GetConnection()
        {
            return _connection;
        }

        private async Task<IAdminClient> CreateAdminClientAsync()
        {
            if (_adminClient != null)
            {
                return _adminClient;
            }

            var options = await _connectionResolver.ResolveAsync(null);
            var uri = options.GetAsString("servers");

            lock (_lock)
            {
                if (_adminClient != null)
                {
                    return _adminClient;
                }

                var opts = new AdminClientConfig
                {
                    ClientId = _clientId,
                    BootstrapServers = uri,
                };

                var username = options.GetAsString("username");
                if (!string.IsNullOrEmpty(username))
                {
                    opts.SaslUsername = username;
                    var password = options.GetAsString("password");
                    opts.SaslPassword = password;

                    var mechanism = options.GetAsString("mechanism");
                    switch (mechanism)
                    {
                        case "scram-sha-256":
                            opts.SaslMechanism = SaslMechanism.ScramSha256;
                            break;
                        case "scram-sha-512":
                            opts.SaslMechanism = SaslMechanism.ScramSha512;
                            break;
                        default:
                            opts.SaslMechanism = SaslMechanism.Plain;
                            break;
                    }
                }

                _adminClient = new AdminClientBuilder(opts).Build();
                return _adminClient;
            }
        }

        /// <summary>
        /// Reads names of available queues or topics.
        /// </summary>
        /// <returns>A list with queue names</returns>
        public async Task<List<string>> ReadQueueNamesAsync()
        {
            CheckOpen();

            var client = await CreateAdminClientAsync();
            var metadata = client.GetMetadata(TimeSpan.FromMilliseconds(30000));

            var topics = new List<string>();
            foreach (var topic in metadata.Topics)
            {
                topics.Add(topic.Topic);
            }

            return topics;
        }

        /// <summary>
        /// Create a queue or topic with a specified name.
        /// </summary>
        /// <param name="name">A name of the queue to be created</param>
        public async Task CreateQueueAsync(string name)
        {
            CheckOpen();

            var client = await CreateAdminClientAsync();

            await client.CreateTopicsAsync(new TopicSpecification[]{
                new TopicSpecification { Name = name, NumPartitions = 1, ReplicationFactor = 1 }
            });
        }

        /// <summary>
        /// Delete a queue or topic with a specified name.
        /// </summary>
        /// <param name="name">A name of the queue to be deleted</param>
        public async Task DeleteQueueAsync(string name)
        {
            CheckOpen();

            var client = await CreateAdminClientAsync();

            await client.DeleteTopicsAsync(new string[]{ name });
        }

        private void CheckOpen()
        {
            if (IsOpen())
                return;

            throw new InvalidStateException(
                null,
                "NOT_OPEN",
                "Connection was not opened"
            );
        }

        public async Task PublishAsync(string topic, Message<byte[], byte[]> message)
        {
            CheckOpen();

            await _connection.ProduceAsync(topic, message);
        }

        public async Task SubscribeAsync(string topic, string groupId, ConsumerConfig config, IKafkaMessageListener listener)
        {
            CheckOpen();

            var options = await _connectionResolver.ResolveAsync(null);
            var uri = options.GetAsString("servers");

            var opts = new ConsumerConfig
            {
                ClientId = _clientId,
                GroupId = groupId,
                BootstrapServers = uri,
                AllowAutoCreateTopics = true,
                Acks = (Acks)_acks,
            };

            if (config != null)
            {
                _autocommitSubscriptions.Add(config.EnableAutoCommit ?? false);
                opts.EnableAutoCommit = config.EnableAutoCommit;
                opts.AutoCommitIntervalMs = config.AutoCommitIntervalMs;
            } else
            {
                _autocommitSubscriptions.Add(false);
            }

            var username = options.GetAsString("username");
            if (!string.IsNullOrEmpty(username))
            {
                opts.SaslUsername = username;
                var password = options.GetAsString("password");
                opts.SaslPassword = password;

                var mechanism = options.GetAsString("mechanism");
                switch (mechanism)
                {
                    case "scram-sha-256":
                        opts.SaslMechanism = SaslMechanism.ScramSha256;
                        break;
                    case "scram-sha-512":
                        opts.SaslMechanism = SaslMechanism.ScramSha512;
                        break;
                    default:
                        opts.SaslMechanism = SaslMechanism.Plain;
                        break;
                }
            }

            // Subscribe to the topic
            var handler = new ConsumerBuilder<byte[], byte[]>(opts).Build();
            handler.Subscribe(topic);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            // Consume incoming messages in background
            ThreadPool.QueueUserWorkItem(delegate {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var result = handler.Consume(cancellationToken);
                        if (!cancellationToken.IsCancellationRequested && result.Message != null)
                        {
                            var message = new KafkaMessage
                            {
                                Message = result.Message,
                                Consumer = handler,
                                Result = result
                            };
                            listener.OnMessage(message);
                        }
                    }
                }
                catch (ThreadInterruptedException)
                {
                    // Do nothing...
                }
                catch (Exception ex)
                {
                    _logger.Error(null, ex, "Failed to consumer messages from Kafka broker");
                }
            });


            lock (_lock)
            {
                // Add subscription to the list
                var subscription = new KafkaSubscription
                {
                    Topic = topic,
                    GroupId = groupId,
                    Listener = listener,
                    Handler = handler,
                    Token = cancellationTokenSource
                };
                _subscriptions.Add(subscription);
            }

            await Task.Delay(0);
        }

        public async Task UnsubscribeAsync(string topic, string groupId, IKafkaMessageListener listener)
        {
            CheckOpen();

            KafkaSubscription deletedSubscription = null;

            lock (_lock)
            {
                // Find subscription
                for (var index = 0; index < _subscriptions.Count; index++)
                {
                    var subscription = _subscriptions[index];
                    if (subscription.Topic == topic && subscription.GroupId == groupId && subscription.Listener == listener)
                    {
                        deletedSubscription = subscription;
                        _subscriptions.RemoveAt(index);
                        _autocommitSubscriptions.RemoveAt(index);
                        break;
                    }
                }
            }

            // Unsubscribe if there are no more subscriptions
            if (deletedSubscription != null)
            {
                deletedSubscription.Token.Cancel(true);
                deletedSubscription.Handler.Unsubscribe();
                deletedSubscription.Handler.Close();
            }

            await Task.Delay(0);
        }

        /// <summary>
        /// Commit a message offset.
        /// </summary>
        /// <param name="topic">a topic name</param>
        /// <param name="groupId">(optional) a consumer group id</param>
        /// <param name="partition">a partition number</param>
        /// <param name="offset">a message offset</param>
        /// <param name="listener">a message listener</param>
        /// <returns></returns>
        public void Commit(string topic, string groupId, int partition, long offset, IKafkaMessageListener listener)
        {
            // Check for open connection
            this.CheckOpen();

            // Find the subscription
            var subscription = _subscriptions.Find((s) => s.Topic == topic && s.GroupId == groupId && s.Listener == listener);
            var autocommit = _autocommitSubscriptions[_subscriptions.IndexOf(subscription)];
            if (subscription == null || autocommit)
                return;

            // Commit the offset
            var topicOffset = new TopicPartitionOffset(
                new TopicPartition(topic, new Partition(partition)),
                new Offset(offset)
            );

            subscription.Handler.Commit(offsets: new List<TopicPartitionOffset> { topicOffset });
        }

        /// <summary>
        /// Seek a message offset.
        /// </summary>
        /// <param name="topic">a topic name</param>
        /// <param name="groupId">(optional) a consumer group id</param>
        /// <param name="partition">a partition number</param>
        /// <param name="offset">a message offset</param>
        /// <param name="listener">a message listener</param>
        /// <returns></returns>
        public void Seek(string topic, string groupId, int partition, long offset, IKafkaMessageListener listener)
        {
            // Check for open connection
            CheckOpen();

            // Find the subscription
            var subscription = _subscriptions.Find((s) => s.Topic == topic && s.GroupId == groupId && s.Listener == listener);
            var autocommit = _autocommitSubscriptions[_subscriptions.IndexOf(subscription)];
            if (subscription == null || autocommit)
                return;

            // Seek the offset
            var topicOffset = new TopicPartitionOffset(
                new TopicPartition(topic, new Partition(partition)),
                new Offset(offset)
            );

            subscription.Handler.Seek(topicOffset);
        }
    }
}
