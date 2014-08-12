namespace Rebus.Transports.Redis
{
    using System;
	using System.Linq;
    using Rebus.Shared;
    using System.IO;
    using StackExchange.Redis;

    /// <summary>
    /// Implementation of a DuplexTransport using Redis List with push / pop operations.
    /// Durability requires AOF enabled in Redis.
    /// </summary>
    public class RedisMessageQueue : IDuplexTransport, IDisposable
    {
        private const string MessageCounterKey = "rebus:message:counter";
	
		private readonly ConnectionMultiplexer redis;
        private readonly string inputQueueName;
	    
		/// <summary>
        /// Initializes a new instance of the <see cref="RedisMessageQueue" /> class.
        /// </summary>
        /// <param name="configOptions">Redis connection configuration options.</param>
        /// <param name="inputQueueName">Name of the input queue.</param>
        public RedisMessageQueue(ConfigurationOptions configOptions, string inputQueueName)
        {
            var tw = new StringWriter();
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

            InternalSend(db, destinationQueueName, redisMessage, expiry, context);
        }

        public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
        {
			IDatabase db = this.redis.GetDatabase();

			var serializedMessage = InternalReceive(db, context);

			if (serializedMessage.IsNull) 
			{
				return null;
			}

			var message = RedisTransportMessage.Deserialize(serializedMessage.ToString());

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

		private void InternalSend(IDatabase db, string destinationQueueName, RedisTransportMessage message, TimeSpan? expiry, ITransactionContext context)
		{
			var serializedMessage = message.Serialize();

			var redisTx = context.IsTransactional ? 
				RedisTransactionContext.GetFromTransactionContext(db, context).Transaction :
				db.CreateTransaction();

			MessageQueueKey messageQueueKey = new MessageQueueKey(destinationQueueName);

			redisTx.StringSetAsync(message.Id, serializedMessage, expiry, When.NotExists);
			redisTx.ListLeftPushAsync(messageQueueKey.Key, message.Id);

			if (!context.IsTransactional) 
			{
				redisTx.Execute();
			} 
		}

		private RedisValue InternalReceive(IDatabase db, ITransactionContext context)
		{
			RedisValue incomingMessageId;

			PurgeTransactionLog();

			MessageQueueKey messageQueue = new MessageQueueKey(this.inputQueueName);

			if (context.IsTransactional) 
			{
				PurgeTransactionLog();

				var transactionContext = RedisTransactionContext.GetFromTransactionContext(db, context);
                MessageQueueTransactionKey messageQueueTransaction = new MessageQueueTransactionKey(this.inputQueueName, transactionContext.TransactionId);
				incomingMessageId = db.ListRightPopLeftPush(messageQueue.Key, messageQueueTransaction.Key, CommandFlags.PreferMaster);

				context.BeforeCommit += () =>  
				{
					// add read messages to delete on commit
					//transactionContext.Transaction.KeyDeleteAsync(incomingMessageId);
				};

				context.AfterRollback += () =>  
				{
					db.ListRightPopLeftPush(messageQueueTransaction.Key, messageQueue.Key, CommandFlags.PreferMaster);
				};
			} 
			else 
			{
				incomingMessageId = db.ListRightPop(messageQueue.Key, CommandFlags.PreferMaster);
			}

			if (incomingMessageId.IsNull)
			{
				// empty queue
				return RedisValue.Null;
			}

			return db.StringGet(incomingMessageId.ToString());
		}

		private void PurgeTransactionLog()
		{
			// TODO: need to do this only "sometimes"; add some kind of waiver on elapsed time;

			IDatabase db = this.redis.GetDatabase();
            IServer server = this.redis.GetServer(this.redis.GetEndPoints().First());

            var transactionQueueKeys = server.Keys(0, MessageQueueTransactionKey.MatchPattern);

			foreach (var transactionQueueKey in transactionQueueKeys)
			{
				var transactionQueue = new MessageQueueTransactionKey(transactionQueueKey);

				if (!RedisTransactionContext.IsTransactionActive(db, transactionQueue.TransactionId))
				{
					MessageQueueKey messageQueue = transactionQueue.MessageQueue;
					db.ListRightPopLeftPush(transactionQueue.Key, messageQueue.Key, CommandFlags.PreferMaster);
				}
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