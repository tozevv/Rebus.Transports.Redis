namespace Rebus.Transports.Redis
{
    using System;
    using Rebus.Shared;
    using StackExchange.Redis;

    /// <summary>
    /// Implementation of a DuplexTransport using Redis List with push / pop operations.
    /// Durability requires AOF enabled in Redis.
    /// </summary>
    public class RedisMessageQueue : IDuplexTransport, IDisposable
    {
        private const string MessageCounterKey = "rebus_message_counter";

        private readonly ConnectionMultiplexer redis;
        private readonly string inputQueueName;

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisMessageQueue" /> class.
        /// </summary>
        /// <param name="configOptions">Redis connection configuration options.</param>
        /// <param name="inputQueueName">Name of the input queue.</param>
        public RedisMessageQueue(ConfigurationOptions configOptions, string inputQueueName)
        {
            var tw = new System.IO.StringWriter();
            try
            {
                this.redis = ConnectionMultiplexer.Connect(configOptions, tw);
            }
            catch
            {
                throw new Exception(tw.ToString());

            }

            this.inputQueueName = inputQueueName;
        }

        public string InputQueue
        {
            get { return this.inputQueueName; }
        }

        public string InputQueueAddress
        {
            get { return this.inputQueueName; }
        }

        public void Send(string destinationQueueName, TransportMessageToSend message, ITransactionContext context)
        {
            IDatabase db = this.redis.GetDatabase();

            var redisMessage = new RedisTransportMessage()
            {
                Id = db.StringIncrement(MessageCounterKey).ToString(),
                Body = message.Body,
                Headers = message.Headers,
                Label = message.Label
            };

            var expiry = GetMessageExpiration(message);

            Send(db, destinationQueueName, redisMessage, expiry, context);
        }

        private void Send(IDatabase db, string destinationQueueName, RedisTransportMessage message, TimeSpan? expiry, ITransactionContext context)
        {
            var serializedMessage = message.Serialize();

            var redisTx = db.CreateTransaction();

            redisTx.StringSetAsync(message.Id, serializedMessage, expiry, When.NotExists);
            redisTx.ListLeftPushAsync(destinationQueueName, message.Id);

            if (!context.IsTransactional)
            {
                redisTx.Execute();
            }
            else
            {
                //defer execution until Rebus transaction is committed
                context.DoCommit += () => redisTx.Execute();
            }
        }

        public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
        {
            //ReceiveMessage isn't transactional yet

            IDatabase db = this.redis.GetDatabase();

            RedisValue incomingMessageId = db.ListRightPop(this.inputQueueName);

            if (incomingMessageId.IsNull)
            {
                return null;
            }

            string messageId = incomingMessageId;

            var serializedMessage = db.StringGet(messageId);

            if (serializedMessage.IsNull)
            {
                //means it has expired
                return null;
            }

            var message = RedisTransportMessage.Deserialize(serializedMessage);

            return new ReceivedTransportMessage()
            {
                Id = message.Id,
                Body = message.Body,
                Headers = message.Headers,
                Label = message.Label
            };
        }

        public void Dispose()
        {
            if (this.redis != null)
            {
                this.redis.Dispose();
            }
        }

        private static TimeSpan? GetMessageExpiration(TransportMessageToSend message)
        {
            object timeoutString = string.Empty;
            if (message.Headers.TryGetValue(Headers.TimeToBeReceived, out timeoutString) &&
                (timeoutString is string))
            {
                return TimeSpan.Parse(timeoutString as string);
            }
            else
            {
                return null;
            }
        }
    }
}
