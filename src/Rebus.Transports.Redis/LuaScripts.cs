namespace Rebus.Transports.Redis
{
    using StackExchange.Redis;
    using System;

    internal static class LuaScripts
    {
        public static void SendMessageAsync(this IDatabaseAsync db, RedisValue serializedMessage, RedisKey queueKey, TimeSpan? expiry)
        {
            // redis expiration needs to be on the nearest "second"
            long expirationInSeconds = Convert.ToInt64(expiry.HasValue ? expiry.Value.TotalSeconds : 0);

            db.ScriptEvaluateAsync(@"
                local message_id = redis.call('INCR', 'rebus:message:counter')
                redis.call('SET', message_id, ARGV[1])
                redis.call('LPUSH', KEYS[1], message_id)

                if tonumber(ARGV[2]) > 0 then
                    redis.call('EXPIRE', message_id, ARGV[2])
                end"
                , new RedisKey[] { queueKey }, 
                new RedisValue[] { serializedMessage, expirationInSeconds });            
        }


        public static RedisValue ReceiveMessage(this IDatabase db,  RedisKey queueKey)
        {
           // var result = a
            RedisResult result = db.ScriptEvaluate(@"
                local message_id = redis.call('RPOP', KEYS[1])
                if (message_id == false) then
                    return false
                else
                    local message = redis.call('GET', message_id)
                    redis.call('DEL', message_id)
                    return message
                end"
                , new RedisKey[] { queueKey });

            return (RedisValue)result;
        }


        public static RedisValue ReceiveMessageInTransaction(this IDatabase db, RedisKey queueKey)
        {
            // var result = a
            RedisResult result = db.ScriptEvaluate(@"
                local message_id = redis.call('RPOP', KEYS[1])
                if (message_id == false) then
                    return false
                else
                    local message = redis.call('GET', message_id)
                    redis.call('DEL', message_id)
                    return message
                end"
                , new RedisKey[] { queueKey });

            return (RedisValue)result;
        }
    }
}
