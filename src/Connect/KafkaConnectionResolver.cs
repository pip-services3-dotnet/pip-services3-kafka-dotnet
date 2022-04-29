using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;

namespace PipServices3.Kafka.Connect
{
    public class KafkaConnectionResolver: IConfigurable, IReferenceable
    {
        private ConnectionResolver _connectionResolver = new ConnectionResolver();
        private CredentialResolver _credentialResolver = new CredentialResolver();

        public KafkaConnectionResolver() { }

        /// <summary>
        /// Configure are configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">Configuration parameters to be set</param>
        public void Configure(ConfigParams config)
        {
            _connectionResolver.Configure(config);
            _credentialResolver.Configure(config);
        }

        /// <summary>
        /// SetReferences are sets references to dependent components.
        /// </summary>
        /// <param name="references">References to be set</param>
        public void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _credentialResolver.SetReferences(references);
        }

        private void ValidateConnection(string correlationId, ConnectionParams connection)
        {
            if (connection == null)
            {
                throw new ConfigException(correlationId, "NO_CONNECTION", "Kafka connection is not set");        
            }

            var uri = connection.Uri;
            if (!string.IsNullOrEmpty(uri))
            {
                return;
            }

            var protocol = connection.GetProtocolWithDefault("tcp");
            if (string.IsNullOrEmpty(protocol))
            {
                throw new ConfigException(correlationId, "NO_PROTOCOL", "Connection protocol is not set");
            }

            var host = connection.Host;
            if (string.IsNullOrEmpty(host))
            {
                throw new ConfigException(correlationId, "NO_HOST", "Connection host is not set");
            }

            var port = connection.GetAsIntegerWithDefault("port", 9092);
            if (port == 0)
            {
                throw new ConfigException(correlationId, "NO_PORT", "Connection port is not set");
            }

            return;
        }

        private ConfigParams ComposeOptions(List<ConnectionParams> connections, CredentialParams credential)
        {
            credential = credential ?? new CredentialParams();

            // Construct options and copy over credentials
            var options = new ConfigParams().SetDefaults(credential);

            var globalUri = "";
            var serversBuilder = new StringBuilder();

            // Process connections, find or construct uri
            foreach (var connection in connections)
            {
                options = options.SetDefaults(connection);

                if (globalUri != "")
                {
                    continue;
                }

                var uri = connection.Uri;
                if (!string.IsNullOrEmpty(uri))
                {
                    globalUri = uri;
                    continue;
                }

                if (serversBuilder.Length > 0)
                {
                    serversBuilder.Append(",");
                }

                var host = connection.Host;
                serversBuilder.Append(host);

                var port = connection.GetAsIntegerWithDefault("port", 9092);
                serversBuilder.Append(":");
                serversBuilder.Append(port.ToString());
            }

            // Set connection uri
            if (globalUri != "")
            {
                var pos = globalUri.IndexOf("://");
                if (pos > 0)
                {
                    globalUri = globalUri.Substring(pos + 3);
                }

                pos = globalUri.IndexOf("@");
                if (pos > 0)
                {
                    var userPass = globalUri.Substring(0, pos);
                    globalUri = globalUri.Substring(pos + 1);
                    pos = userPass.IndexOf(":");
                    if (pos > 0)
                    {
                        var username = userPass.Substring(0, pos);
                        options.SetAsObject("username", username);
                        var password = userPass.Substring(pos + 1);
                        options.SetAsObject("password", password);

                    }
                    else
                    {
                        options.SetAsObject("username", userPass);
                    }
                }

                pos = globalUri.IndexOf("?");
                if (pos > 0)
                {
                    globalUri = globalUri.Substring(0, pos);
                }

                options.SetAsObject("servers", globalUri);
            }
            else
            {
                options.SetAsObject("servers", serversBuilder.ToString());
            }

            return options;
        }

        /// <summary>
        /// Resolves Kafka connection options from connection and credential parameters.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <returns>Composed connection parameters</returns>
        public async Task<ConfigParams> ResolveAsync(string correlationId)
        {
            var connections = await _connectionResolver.ResolveAllAsync(correlationId);
            var credential = await _credentialResolver.LookupAsync(correlationId);

            // Validate connections
            foreach (var connection in connections)
            {
                ValidateConnection(correlationId, connection);
            }

            var options = ComposeOptions(connections, credential);
            return options;
        }

        /// <summary>
        ///  Compose method are composes Kafka connection options from connection and credential parameters.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="connections">Connection parameters</param>
        /// <param name="credential">Credential parameters</param>
        /// <returns>Composed connection parameters</returns>
        public ConfigParams Compose(string correlationId, List<ConnectionParams> connections, CredentialParams credential)
        {
            // Validate connections
            foreach (var connection in connections)
            {
                ValidateConnection(correlationId, connection);
            }

            var options = ComposeOptions(connections, credential);
            return options;
        }
    }
}