namespace Rebus.Transports.Redis
{
    using MsgPack.Serialization;
    using StackExchange.Redis;
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;

	/// <summary>
	/// Implementation of a DuplexTransport using Redis List with push / pop operations.
	/// Durability requires AOF enabled in Redis.
	/// 
	/// 
	/// if context is transactional the algorithm works like this:
	/// - message ids are read and copied to a rollback queue associated with the transaction in an atomic operation
	/// - commit: messages with a specific key id are removed
	/// - rollback: messages are atomically copied from rollback queue back to the queue in an atomic operation
	/// - failure to commit / abort: purge rollback log moves messages from rollback queue back to the queue in an atomic operation
	///   a failure is detected via a transaction timeout.
	/// </summary>
	public class RedisMessageQueue : IDuplexTransport, IDisposable
	{
		private const string MessageCounterKeyFormat = "rebus:message:counter";
		private const string QueueKeyFormat = "rebus:queue:{0}";
		private const string RollbackQueueKeyFormat = "rebus:queue:{0}:rollback:{1}";
		private readonly ConnectionMultiplexer redis;
		private readonly string inputQueueName;
        private readonly MessagePackSerializer<RedisTransportMessage> serializer;

		/// <summary>
		/// Initializes a new instance of the <see cref="RedisMessageQueue" /> class.
		/// </summary>
		/// <param name="configOptions">Redis connection configuration options.</param>
		/// <param name="inputQueueName">Name of the input queue.</param
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
            this.serializer = MessagePackSerializer.Get<RedisTransportMessage>();
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
            RedisKey queueKey = string.Format(QueueKeyFormat, destinationQueueName);
            var txManager = context.GetTransactionManager(db);
      
			var id = db.StringIncrement(MessageCounterKeyFormat);

            var redisMessage = new RedisTransportMessage(id.ToString(), message);
            var expiry = redisMessage.GetMessageExpiration();

            var serializedMessage = this.serializer.PackSingleObject(redisMessage);

         
            var tx = txManager.CommitTx;
            tx.StringSetAsync(redisMessage.Id, serializedMessage, expiry, When.Always);
            tx.ListLeftPushAsync(queueKey, redisMessage.Id);
            
            if (!context.IsTransactional) 
            {
                tx.Execute();
            }
		}

		public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
		{
			IDatabase db = this.redis.GetDatabase();
            RedisKey queueKey = string.Format(QueueKeyFormat, this.inputQueueName);
            var txManager = context.GetTransactionManager(db);
            byte[] serializedMessage = null;            

            if (context.IsTransactional)
            {
                // purge rollback log from previous calls
                #pragma warning disable 4014
                PurgeRollbackLog();
                #pragma warning restore 4014
                
                // atomically copy message id from queue to specific transaction rollback queue
                RedisKey rollbackQueueKey = string.Format(RollbackQueueKeyFormat, this.inputQueueName, txManager.TransactionId);
                RedisValue incomingMessageId = db.ListRightPopLeftPush(queueKey, rollbackQueueKey, CommandFlags.PreferMaster);
                db.KeyExpire(rollbackQueueKey, TimeSpan.FromSeconds(30));

                if (incomingMessageId.IsNull)
                {
                    return null;
                }

                // ok, a message was read and the transaction commited
                // prepater the key for deletion in the single transaction commit
                txManager.CommitTx.KeyDeleteAsync(incomingMessageId.ToString());
                txManager.CommitTx.ListRightPopAsync(rollbackQueueKey);

                // atomically prepare rollback, moving the message id back to the queue
                txManager.RollbackTx.ListRightPopLeftPushAsync(rollbackQueueKey, queueKey, CommandFlags.PreferMaster);

                serializedMessage = db.StringGet(incomingMessageId.ToString());
            }
            else
            {
                // no transaction here, just retrieve the key id from que Redis list
                RedisValue incomingMessageId = db.ListRightPop(queueKey, CommandFlags.None);

                if (incomingMessageId.IsNull)
                {
                    return null;
                }

                serializedMessage = db.StringGet(incomingMessageId.ToString());

                #pragma warning disable 4014
                db.KeyDeleteAsync(incomingMessageId.ToString());
                #pragma warning restore 4014
            }

            if (serializedMessage == null)
            {
                return null; // probably expired....
            }
			var message = this.serializer.UnpackSingleObject(serializedMessage);
            return message.ToReceivedTransportMessage();
		}

		public void Dispose()
		{
			if (this.redis != null)
			{
				this.redis.Dispose();
			}
		}

		private async Task PurgeRollbackLog()
		{
            /*
			IDatabase db = this.redis.GetDatabase();
			IServer server = this.redis.GetServer(this.redis.GetEndPoints().First());

			// search all existing rollback queues 
			// and move 
			var rollbackQueues = server.Keys(0, string.Format(RollbackQueueKeyFormat, "*", "*"));

			foreach (var rollbackQueueKey in rollbackQueues)
			{
				string rollbackQueueString = rollbackQueueKey;

				string inputQueueName = rollbackQueueString.ParseFormat(RollbackQueueKeyFormat, 0);
				long transactionId = long.Parse(rollbackQueueString.ParseFormat(RollbackQueueKeyFormat, 1));
         
				if (!RedisTransactionManager.IsTransactionActive(db, transactionId))
				{
					RedisKey queueKey = string.Format(QueueKeyFormat, inputQueueName);

					// rollback all messages on the rollback queue.
					while (await db.ListRightPopLeftPushAsync(rollbackQueueKey, queueKey, CommandFlags.PreferMaster) != RedisValue.Null)
					{
					}
				}
			}*/
		}
	}
}