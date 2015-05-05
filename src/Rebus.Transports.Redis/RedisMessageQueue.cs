namespace Rebus.Transports.Redis
{
    using System;
    using System.IO;
    using System.Linq;
    using MsgPack.Serialization;
    using StackExchange.Redis;

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
        private readonly MessagePackSerializer<RedisTransportMessage> serializer;
        private readonly string inputQueueName;
        private readonly string inputQueueKey;
        private readonly TimeSpan transactionTimeout;

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisMessageQueue" /> class.
        /// </summary>
        /// <param name="configOptions">Redis connection configuration options.</param>
        /// <param name="inputQueueName">Name of the input queue.</param>
        /// <param name="transactionTimeout">Transaction timeout to use if transactions enable.</param>
        public RedisMessageQueue(ConfigurationOptions configOptions, string inputQueueName, TimeSpan? transactionTimeout = null)
        {
            var tw = new StringWriter();
            try
            {
                this.redis = ConnectionMultiplexer.Connect(configOptions, tw);
            }
            catch (Exception ex)
            {
                throw new Exception(tw.ToString(), ex);

            }
            this.serializer = MessagePackSerializer.Get<RedisTransportMessage>();
            this.inputQueueName = inputQueueName;
            this.inputQueueKey = string.Format(QueueKeyFormat, this.inputQueueName);
            this.transactionTimeout = transactionTimeout ?? TimeSpan.FromSeconds(30);
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
            RedisKey destinationQueueKey = string.Format(QueueKeyFormat, destinationQueueName);
           
            if (context.IsTransactional)
            {
                var tx = RedisTransactionManager.GetOrCreate(context, db, transactionTimeout).CommitTx;
                InternalSend(tx, destinationQueueKey, message);
            }
            else
            {
                InternalSend(db, destinationQueueKey, message);
            }
        }

        public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
        {
            IDatabase db = this.redis.GetDatabase();

            if (context.IsTransactional)
            {
                var txManager = RedisTransactionManager.GetOrCreate(context, db, transactionTimeout);

                // purge rollback log from previous calls
                CleanupRollbacks();
               
                RedisKey rollbackQueueKey = string.Format(RollbackQueueKeyFormat, this.inputQueueName, txManager.TransactionId);
                RedisKey transactionSetKey = string.Format(TransactionSetKeyFormat, this.inputQueueName);

                var message = InternalReceiveMessageInTransaction(db, this.inputQueueKey, 
                                                                 rollbackQueueKey, transactionSetKey, 
                                                                 txManager.TransactionId);

                if (message == null) 
                {
                    return null;
                }

                // ok, a message was read and the transaction commited
                // prepare the key for deletion in the single transaction commit

                txManager.CommitTx.KeyDeleteAsync(message.Id);
                txManager.CommitTx.ListRightPopAsync(rollbackQueueKey);

                // atomically prepare rollback, moving the message id back to the queue
                txManager.RollbackTx.ListRightPopLeftPushAsync(rollbackQueueKey, this.inputQueueKey, CommandFlags.PreferMaster);

                return message;
            }
            else
            {
                // no transaction here, just retrieve the key id from que Redis list
                return InternalReceiveMessage(db, this.inputQueueKey);
            }
        } 

        public void Dispose()
        {
            if (this.redis != null)
            {
                this.redis.Dispose();
            }
        }

        private void InternalSend(IDatabaseAsync db, string destinationQueueKey, TransportMessageToSend message)
        {
            var redisMessage = new RedisTransportMessage(message);
            var expiry = redisMessage.GetMessageExpiration();
            RedisValue serializedMessage = this.serializer.PackSingleObject(redisMessage);

            // redis expiration needs to be on the nearest "second"
            long expirationInSeconds = Convert.ToInt64(expiry.HasValue ? expiry.Value.TotalSeconds : 0);

            db.ScriptEvaluateAsync(@"
                -- increment sequential message id
                local message_id = redis.call('INCR', 'rebus:message:counter')

                -- create message key with proper expiration
                redis.call('SET', message_id, ARGV[1])
               
                if tonumber(ARGV[2]) > 0 then
                    redis.call('EXPIRE', message_id, ARGV[2])
                end

                -- push message id to queue
                redis.call('LPUSH', KEYS[1], message_id)"
                    , new RedisKey[] { destinationQueueKey }, 
                new RedisValue[] { serializedMessage, expirationInSeconds });            
        }

        private ReceivedTransportMessage InternalReceiveMessage(IDatabase db, RedisKey queueKey)
        {
            RedisResult result = db.ScriptEvaluate(@"
                -- get message id from the queue
                local message_id = redis.call('RPOP', KEYS[1])
                if (message_id == false) then
                    return false
                else
                    -- get message from message id
                    local message = redis.call('GET', message_id)
                    redis.call('DEL', message_id)
                    return { message_id, message } 
                end"
            , new RedisKey[] { queueKey });

            return ParseMessage(result);
        }

        private ReceivedTransportMessage InternalReceiveMessageInTransaction(IDatabase db, RedisKey queueKey,
                                                                             RedisKey rollbackQueueKey, RedisKey transactionSetKey, RedisValue transactionId)
        {
            RedisResult result = db.ScriptEvaluate(@"
                -- get message id from the queue and move it to the rollback queue
                local message_id = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
                if (message_id == false) then
                    return false
                else
                    -- add newly create rollback queue to set of rollback queues
                    redis.call('SADD', KEYS[3], ARGV[1])

                    -- get message from message id
                    local message = redis.call('GET', message_id)

                    -- delete message is done only on transaction commit
                    return { message_id, message }
                end"
            , new RedisKey[] { queueKey, rollbackQueueKey, transactionSetKey }, new RedisValue[] { transactionId });

            return ParseMessage(result);
        }

        private ReceivedTransportMessage ParseMessage(RedisResult result) 
        {
            if (result.IsNull)
            {
                return null;
            }
            RedisValue[] messageAndId = (RedisValue[])result;
            string messageId = (string)messageAndId[0];
            byte[] serializedMessage = (byte[])messageAndId[1];

            if (serializedMessage == null)
            {
                return null; // probably expired
            }
            return this.serializer
                .UnpackSingleObject(serializedMessage)
                .ToReceivedTransportMessage(messageId);
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