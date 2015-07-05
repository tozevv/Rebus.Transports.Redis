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
            RedisKey destinationQueueKey = string.Format(QueueKeyFormat, destinationQueueName);
           
            var redisMessage = new RedisTransportMessage(message);
            RedisValue serializedMessage = this.serializer.PackSingleObject(redisMessage);

            // redis expiration needs to be on the nearest "second"
            var expiry = redisMessage.GetMessageExpiration();
            long expirationInSeconds = Convert.ToInt64(expiry.HasValue ? expiry.Value.TotalSeconds : 0);


            IDatabase db = this.redis.GetDatabase();

            string script = @"
                -- increment sequential message id
                local message_id = redis.call('INCR', 'rebus:message:counter')

                -- create message key with proper expiration
                redis.call('SET', message_id, ARGV[1])
               
                if tonumber(ARGV[2]) > 0 then
                    redis.call('EXPIRE', message_id, ARGV[2])
                end

                -- push message id to queue
                redis.call('LPUSH', KEYS[1], message_id)";
                
            if (context.IsTransactional)
            {
                GetTransaction(context).ScriptEvaluateAsync(script, 
                    new RedisKey[] { destinationQueueKey }, 
                    new RedisValue[] { serializedMessage, expirationInSeconds });
            }
            else
            {
                db.ScriptEvaluateAsync(script, 
                    new RedisKey[] { destinationQueueKey }, 
                    new RedisValue[] { serializedMessage, expirationInSeconds });
            }
        }

        public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
        {
            if (context.IsTransactional)
            {
                RedisCompensatingTransaction transaction = GetTransaction(context);

                RedisResult result = transaction.ScriptEvaluate(
                    @"
                    -- get message id from the queue and move it to the rollback queue
                    local message_id = redis.call('RPOP', KEYS[1])

                    if (message_id == false) then
                        return false
                    else
                       
                        local message = redis.call('GET', message_id)
                        local expires = redis.call('TTL', message_id) 
                        redis.call('DEL', message_id)
                      
                        compensate('EXPIRE', message_id, expires)
                        compensate('SET', message_id, message)
                        compensate('LPUSH', KEYS[1], message_id)

                        return { message_id, message }
                    end"
                , new RedisKey[] { this.inputQueueKey });

                return ParseMessage(result);
            }
            else
            {
                IDatabase db = this.redis.GetDatabase();

                // no transaction here, just retrieve the key id from que Redis list
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
                , new RedisKey[] { this.inputQueueKey });

                return ParseMessage(result);
            }
        } 

        public void Dispose()
        {
            if (this.redis != null)
            {
                this.redis.Dispose();
            }
        }
          
        private RedisCompensatingTransaction GetTransaction(ITransactionContext context) 
        {
            const string RedisContextKey = "redis:context";

            if (! context.IsTransactional)
            {
                return null;
            }

            // locking not needed here 
            // assuming 1-to-1 relathionship between current worker and context
            var redisTransaction = context[RedisContextKey] as RedisCompensatingTransaction;
            if (redisTransaction == null)
            {
                var db = this.redis.GetDatabase();
                redisTransaction = db.BeginCompensatingTransaction(this.transactionTimeout);

                context.DoCommit += () => {
                    redisTransaction.Commit();
                };
                context.DoRollback += () => {
                    redisTransaction.Rollback();
                };
                context[RedisContextKey] = redisTransaction;
            }
            return redisTransaction;

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
       
      /*  private void CleanupRollbacks()
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
        }*/
    }
}