namespace Rebus.Transports.Redis
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;
	using System.Threading.Tasks;
	using MsgPack.Serialization;
	using Rebus.Shared;
	using StackExchange.Redis;

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
		private const string RedisContextKey = "redis_context";

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
      
			var id = db.StringIncrement(MessageCounterKeyFormat);

			var redisMessage = new RedisTransportMessage()
			{
				Id = id.ToString(),
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

			var message = this.serializer.UnpackSingleObject(serializedMessage);

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
			var serializedMessage = this.serializer.PackSingleObject(message);

			RedisKey queueKey = string.Format(QueueKeyFormat, destinationQueueName);

			if (context.IsTransactional)
			{
				var commitTx = GetRedisTransaction(db, context).CommitTx;
				commitTx.StringSetAsync(message.Id, serializedMessage, expiry, When.Always);
				commitTx.ListLeftPushAsync(queueKey, message.Id);
			}
			else
			{

                var commitTx = db.CreateTransaction();
                commitTx.StringSetAsync(message.Id, serializedMessage, expiry, When.Always);
                commitTx.ListLeftPushAsync(queueKey, message.Id);
                commitTx.Execute();
                /*
                db.StringSet(message.Id, serializedMessage, expiry, When.Always);
                db.ListLeftPush(queueKey, message.Id);*/
			}
		}

		public RedisValue InternalReceive(IDatabase db, ITransactionContext context)
		{
			RedisValue incomingMessageId;
			RedisKey queueKey = string.Format(QueueKeyFormat, this.inputQueueName);

			if (context.IsTransactional)
			{
				// purge rollback log from previous calls
                #pragma warning disable 4014
				PurgeRollbackLog();
                #pragma warning restore 4014
				var redisTransaction = GetRedisTransaction(db, context);
   
				// atomically copy message id from queue to specific transaction rollback queue
				RedisKey rollbackQueueKey = string.Format(RollbackQueueKeyFormat, this.inputQueueName, redisTransaction.TransactionId);    


				incomingMessageId = db.ListRightPopLeftPush(queueKey, rollbackQueueKey, CommandFlags.PreferMaster);
                db.KeyExpire(rollbackQueueKey, TimeSpan.FromSeconds(30));


				if (incomingMessageId.IsNull)
				{
					return RedisValue.Null;
				}

				// ok, a message was read and the transaction commited
				// schedule the key for deletion in the single transaction commit
				
				redisTransaction.CommitTx.KeyDeleteAsync(incomingMessageId.ToString());
				redisTransaction.CommitTx.ListRightPopAsync(rollbackQueueKey);
				
					// atomically rollback, moving the message id back to the queue
				redisTransaction.RollbackTx.ListRightPopLeftPushAsync(rollbackQueueKey, queueKey, CommandFlags.PreferMaster);
			

				var message = db.StringGet(incomingMessageId.ToString());

				return message;
			}
			else
			{
				// no transaction here, just retrieve the key id from que Redis list
				incomingMessageId = db.ListRightPop(queueKey, CommandFlags.None);

				if (incomingMessageId.IsNull)
				{
					// empty queue
					return RedisValue.Null;
				}
				string messageKey = incomingMessageId.ToString();
				var message = db.StringGet(messageKey);
				#pragma warning disable 4014
				db.KeyDeleteAsync(messageKey);
				#pragma warning restore 4014
				return message;
		
			}
		}

		private async Task PurgeRollbackLog()
		{
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
         
				if (!RedisTransaction.IsTransactionActive(db, transactionId))
				{
					RedisKey queueKey = string.Format(QueueKeyFormat, inputQueueName);

					// rollback all messages on the rollback queue.
					while (await db.ListRightPopLeftPushAsync(rollbackQueueKey, queueKey, CommandFlags.PreferMaster) != RedisValue.Null)
					{
					}
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

		private static RedisTransaction GetRedisTransaction(IDatabase db, ITransactionContext context)
		{
			if (!context.IsTransactional)
			{
				throw new ArgumentException("Context is not transactional");
			}

			// locking not needed here 
			// assuming 1-to-1 relathionship between current worker and context
			var redisTransaction = context[RedisContextKey] as RedisTransaction;
			if (redisTransaction == null)
			{
				redisTransaction = new RedisTransaction(db, TimeSpan.FromMinutes(1));

				context[RedisContextKey] = redisTransaction;

				context.DoCommit += () =>
				{
					redisTransaction.Commit();
				};

				context.DoRollback += () =>
				{
					redisTransaction.Rollback();
				};
			}
			return redisTransaction;
		}
	}
}