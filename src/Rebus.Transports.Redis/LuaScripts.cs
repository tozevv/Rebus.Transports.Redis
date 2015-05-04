namespace Rebus.Transports.Redis
{
    using StackExchange.Redis;
    using System;

    internal static class LuaScripts
    {
       

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
                    return message_id, message
                end"
                , new RedisKey[] { queueKey });

            return (RedisValue[])result;
        }

        public static RedisValue[] ScriptReceiveMessageAndCopyToRollbackQueue(this IDatabase db,  
            RedisKey queueKey, RedisKey rollbackQueueKey, RedisKey transactionSetKey, RedisValue transactionId)
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
                    return message_id, message
                end"
            , new RedisKey[] { queueKey, rollbackQueueKey, transactionSetKey }, new RedisValue[] { transactionId });

            return (RedisValue[])result;
        }
    }
}
