namespace Rebus.Transports.Redis
{
    using StackExchange.Redis;
    using System;

    internal static class LuaScripts
    {
        public static void ScriptSendMessageAsync(this IDatabaseAsync db, RedisValue serializedMessage, RedisKey queueKey, TimeSpan? expiry)
        {
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

                , new RedisKey[] { queueKey }, 
                new RedisValue[] { serializedMessage, expirationInSeconds });            
        }

        public static RedisValue ScriptReceiveMessage(this IDatabase db,  RedisKey queueKey)
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
                    return message
                end"
                , new RedisKey[] { queueKey });

            return (RedisValue)result;
        }

        public static RedisValue ScriptMoveMessageToRollbackQueue(this IDatabase db,  
            RedisKey queueKey, RedisKey rollbackQueueKey, RedisKey transactionSetKe, RedisValue transactionId)
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
                    redis.call('DEL', message_id)
                    return message
                end"
            , new RedisKey[] { queueKey, rollbackQueueKey, transactionSetKey }, new RedisValue[] { transactionId });

            return (RedisValue)result;
        }
    }
}
