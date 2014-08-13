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
                Id = db.StringIncrement(MessageCounterKeyFormat).ToString(),
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

            RedisKey queueKey = string.Format(QueueKeyFormat, destinationQueueName);

            redisTx.StringSetAsync(message.Id, serializedMessage, expiry, When.NotExists);
            redisTx.ListLeftPushAsync(queueKey, message.Id);

            if (!context.IsTransactional)
            {
                redisTx.Execute();
            } 
        }

        private RedisValue InternalReceive(IDatabase db, ITransactionContext context)
        {
            RedisValue incomingMessageId;

            PurgeRollbackLog();

            RedisKey queueKey = string.Format(QueueKeyFormat, this.inputQueueName);

            if (context.IsTransactional)
            {
                // purge rollback log from previous calls
                PurgeRollbackLog();

                var transactionContext = RedisTransactionContext.GetFromTransactionContext(db, context);
   
                // atomically copy message id from queue to specific transaction rollback queue
                RedisKey rollbackQueueKey = string.Format(RollbackQueueKeyFormat, this.inputQueueName, transactionContext.TransactionId);    
                incomingMessageId = db.ListRightPopLeftPush(queueKey, rollbackQueueKey, CommandFlags.PreferMaster);

                if (!incomingMessageId.IsNull)
                {
                    // ok, a message was read and the transaction commited
                    // schedule the key for deletion in the single transaction commit
                    context.BeforeCommit += () =>
                    {
                        // add read messages to delete on commit
                        transactionContext.Transaction.KeyDeleteAsync(incomingMessageId.ToString());
                    };

                    context.AfterRollback += () =>
                    {
                        // atomically rollback, moving the message id back to the 
                        db.ListRightPopLeftPush(rollbackQueueKey, queueKey, CommandFlags.PreferMaster);
                    };
                }            
            }
            else
            {
                // no transaction here, just retrieve the key id from que Redis list
                incomingMessageId = db.ListRightPop(queueKey, CommandFlags.PreferMaster);
            }

            if (incomingMessageId.IsNull)
            {
                // empty queue
                return RedisValue.Null;
            }

            return db.StringGet(incomingMessageId.ToString());
        }

        private void PurgeRollbackLog()
        {
            // TODO: need to do this only "sometimes"; add some kind of waiver on elapsed time;

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
         
                if (!RedisTransactionContext.IsTransactionActive(db, transactionId))
                {
                    RedisKey queueKey = string.Format(QueueKeyFormat, inputQueueName);

                    // rollback all messages on the rollback queue.
                    while (db.ListRightPopLeftPush(rollbackQueueKey, queueKey, CommandFlags.PreferMaster) != RedisValue.Null)
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
    }
}