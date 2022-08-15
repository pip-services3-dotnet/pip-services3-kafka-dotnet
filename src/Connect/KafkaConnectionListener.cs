using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Run;
using PipServices3.Components.Log;
using System;
using System.Threading.Tasks;

namespace PipServices3.Kafka.Connect
{
    /// <summary>
    /// Kafka connection listener that helps detect if connection to kafka is lost in during of container work
    /// The component starts in backgroud and check kafka connection with interval (default: check_interval=true) 
    /// and try reopen connection if configured(default: reconnect= true)
    /// 
    /// ### Configuration parameters ###
    /// 
    /// - correlation_id:        (optional) transaction id to trace execution through call chain (default: KafkaConnectionListener).
    /// - options:
    ///    - reconnect(default: true)
    ///    - check_interval(default: 1m)
    ///    
    /// ### References ###
    /// - *:logger:*:*:1.0              (optional) <see cref="ILogger"/> components to pass log messages
    /// - *:connection:kafka:\*:1.0     (optional) Shared connection to Kafka service
    /// </summary>
    public class KafkaConnectionListener : IOpenable, IConfigurable, IReferenceable
    {
        private static ConfigParams _defaultConfig = ConfigParams.FromTuples(
            "correlation_id", "KafkaConnectionListener",
            "options.log_level", 1,
            "options.reconnect", true,
            "options.check_interval", 60000
        );

        protected object _lock = new object();

        /// <summary>
        /// The logger.
        /// </summary>
        protected CompositeLogger _logger = new CompositeLogger();

        private FixedRateTimer timer;
        private KafkaConnection connection;

        /// <summary>
        /// Flag for reconection to kafka
        /// </summary>
        private bool _reconnect;

        /// <summary>
        /// Delay of check and reconnect try
        /// </summary>
        private int _checkInerval;

        private string _correlationId;

        /// <summary>
        /// Configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">configuration parameters to be set.</param>
        public void Configure(ConfigParams config)
        {
            config = config.SetDefaults(KafkaConnectionListener._defaultConfig);

            _correlationId = config.GetAsString("correlation_id");
            _reconnect = config.GetAsBoolean("options.reconnect");
            _checkInerval = config.GetAsInteger("options.check_interval");
        }

        /// <summary>
        /// Sets references to dependent components.
        /// </summary>
        /// <param name="references">references to locate the component dependencies. </param>
        public void SetReferences(IReferences references)
        {
            _logger.SetReferences(references);
            connection = references.GetOneRequired<KafkaConnection>(new Descriptor("pip-services", "connection", "kafka", "*", "*"));
        }

        /// <summary>
        /// Checks if connection listener is open
        /// </summary>
        /// <returns>bool flag</returns>
        public bool IsOpen()
        {
            return timer != null;
        }

        /// <summary>
        /// Opens the component.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public Task OpenAsync(string correlationId)
        {
            if (IsOpen())
                return Task.Delay(0);

            timer = new FixedRateTimer(this.checkConnection, this._checkInerval, this._checkInerval);
            timer.Start();

            return Task.Delay(0);
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public Task CloseAsync(string correlationId)
        {
            if (IsOpen())
            {
                timer.Stop();
                timer = null;
            }

            return Task.Delay(0);
        }

        private void checkConnection()
        {
            try
            {
                // try to get topics list
                connection.ReadQueueNamesAsync().Wait();
            }
            catch (Exception)
            {
                _logger.Trace(_correlationId, "Kafka connection is lost");

                // Recreate connection
                if (_reconnect)
                {
                    lock (_lock)
                    {
                        _logger.Trace(_correlationId, "Try Kafka reopen connection");

                        connection.CloseAsync(_correlationId).Wait();
                        connection.OpenAsync(_correlationId).Wait();
                    }
                }
            }
        }
    }
}
