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
			var tw = new System.IO.StringWriter ();
			try {
				this.redis = ConnectionMultiplexer.Connect(configOptions, tw);
			} catch {
				throw new Exception(tw.ToString ());

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
           
            RedisTransportStore redisStore = new RedisTransportStore() 
            {
                Id = db.StringIncrement(MessageCounterKey).ToString(),
                Body = message.Body,
                Headers = message.Headers,
                Label = message.Label,
                ExpirationDateUtc = GetMessageExpirationDateUtc(message)
            };
                
            db.ListLeftPush(destinationQueueName, redisStore.Serialize());
        }

        public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
        {
            IDatabase db = this.redis.GetDatabase();

            RedisValue item = db.ListRightPop(this.inputQueueName);

            if (item == RedisValue.Null)
            {
                return null;
            }

            RedisTransportStore store = RedisTransportStore.Deserialize(item.ToString());

            if (store.ExpirationDateUtc >= DateTime.UtcNow)
            {
                return new ReceivedTransportMessage()
                {
                    Id = store.Id,
                    Body = store.Body,
                    Headers = store.Headers,
                    Label = store.Label
                };
            }
            else
            {
                return null;
            }
        }

        public void Dispose()
        {
            if (this.redis != null)
            {
                this.redis.Dispose();
            }
        }

        private static DateTime GetMessageExpirationDateUtc(TransportMessageToSend message)
        {
            object timeoutString = string.Empty;
            if (message.Headers.TryGetValue(Headers.TimeToBeReceived, out timeoutString) &&
                (timeoutString is string))
            {
                return DateTime.UtcNow.Add(TimeSpan.Parse(timeoutString as string));
            }
            else
            {
                return DateTime.MaxValue;
            }
        }
    }
}
