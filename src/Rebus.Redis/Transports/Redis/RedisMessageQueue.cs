namespace Rebus.Transports.Redis
{
    using MsgPack.Serialization;
    using StackExchange.Redis;
    using System;
    using System.IO;
    using System.Linq;

    /// <summary>
	/// Implementation of a DuplexTransport using Redis List with push / pop operations.
	/// Durability requires AOF enabled in Redis.
	/// </summary>
	public class RedisMessageQueue : IDuplexTransport, IDisposable
	{
		private const string QueueKeyFormat = "rebus:queue:{0}";
		private const string RollbackQueueKeyFormat = "rebus:queue:{0}:rollback:{1}";
        private const string TransactionSetKeyFormat = "rebus:queue:{0}:transactions";

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

            var redisMessage = new RedisTransportMessage(message);
            var expiry = redisMessage.GetMessageExpiration();
            var serializedMessage = this.serializer.PackSingleObject(redisMessage);

            var tx = txManager.CommitTx;

            tx.SendMessageAsync(serializedMessage, queueKey, expiry);
 
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
            string messageId = null;

            if (context.IsTransactional)
            {
                // purge rollback log from previous calls
                #pragma warning disable 4014
                CleanupRollbacks();
                #pragma warning restore 4014
                
                // atomically copy message id from queue to specific transaction rollback queue
              
                RedisValue incomingMessageId;
 
                RedisKey rollbackQueueKey = string.Format(RollbackQueueKeyFormat, this.inputQueueName, txManager.TransactionId);
                RedisKey transactionSetKey = string.Format(TransactionSetKeyFormat, this.inputQueueName);

                db.SetAdd(transactionSetKey, txManager.TransactionId);
                incomingMessageId = db.ListRightPopLeftPush(queueKey, rollbackQueueKey, CommandFlags.PreferMaster);
                
                if (incomingMessageId.IsNull)
                {
                    return null;
                }

                messageId = incomingMessageId.ToString();

                // ok, a message was read and the transaction commited
                // prepater the key for deletion in the single transaction commit
                txManager.CommitTx.KeyDeleteAsync(messageId);
                txManager.CommitTx.ListRightPopAsync(rollbackQueueKey);

                // atomically prepare rollback, moving the message id back to the queue
                txManager.RollbackTx.ListRightPopLeftPushAsync(rollbackQueueKey, queueKey, CommandFlags.PreferMaster);

                serializedMessage = db.StringGet(messageId);
            }
            else
            {
                // no transaction here, just retrieve the key id from que Redis list
                RedisValue incomingMessageId = db.ListRightPop(queueKey, CommandFlags.None);

                if (incomingMessageId.IsNull)
                {
                    return null;
                }

                messageId = incomingMessageId.ToString();

                serializedMessage = db.StringGet(messageId);

                #pragma warning disable 4014
                db.KeyDeleteAsync(messageId);
                #pragma warning restore 4014
            }

            if (serializedMessage == null)
            {
                return null; // probably expired....
            }
			var message = this.serializer.UnpackSingleObject(serializedMessage);
            return message.ToReceivedTransportMessage(messageId);
		}

		public void Dispose()
		{
			if (this.redis != null)
			{
				this.redis.Dispose();
			}
		}

		private void CleanupRollbacks()
		{
            IDatabase db = this.redis.GetDatabase();
            RedisKey transactionSetKey = string.Format(TransactionSetKeyFormat, this.inputQueueName);
        
            RedisKey queueKey = string.Format(QueueKeyFormat, this.inputQueueName);
            var transactionIds = db.SetMembers(transactionSetKey).Select(t => (long)t);

            foreach (var transactionId in transactionIds)
            {
                if (!RedisTransactionManager.IsTransactionActive(db, transactionId))
                {
                    RedisKey rollbackQueueKey = string.Format(RollbackQueueKeyFormat, this.inputQueueName, transactionId);
                    while (db.ListRightPopLeftPush(rollbackQueueKey, queueKey, CommandFlags.PreferMaster) != RedisValue.Null)
                    {

                    }
                }
                db.SetRemove(TransactionSetKeyFormat, new RedisValue[] { transactionId });
            }
        }
	}
}