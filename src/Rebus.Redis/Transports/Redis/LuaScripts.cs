namespace Rebus.Transports.Redis
{

    using StackExchange.Redis;
    using System;

    internal static class LuaScripts
    {
        private const string sendMessageScript = @"
                local message_id = redis.call('INCR', 'rebus:message:counter')
                redis.call('SET', message_id, ARGV[1])
                redis.call('LPUSH', KEYS[1], message_id)

                if tonumber(ARGV[2]) > 0 then
                    redis.call('EXPIRE', message_id, ARGV[2])
                end";

        public static void SendMessageAsync(this IDatabaseAsync db, RedisValue serializedMessage, RedisKey queueKey, TimeSpan? expiry)
        {
            // redis expiration needs to be on the nearest "second"
            long expirationInSeconds = Convert.ToInt64(expiry.HasValue ? expiry.Value.TotalSeconds : 0);

            db.ScriptEvaluateAsync(sendMessageScript
                , new RedisKey[] { queueKey }, new RedisValue[] { serializedMessage, expirationInSeconds });
            
        }
    }
}
