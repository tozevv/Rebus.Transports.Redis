using System;
using StackExchange.Redis;

namespace Rebus.Transports.Redis
{
    public static class IDatabaseExtensions
    {
        public static RedisCompensatingTransaction BeginCompensatingTransaction(this IDatabase database, TimeSpan? expiration = null)
        {
            expiration = expiration.HasValue ? expiration : TimeSpan.FromSeconds(30);
            return new RedisCompensatingTransaction(database, expiration.Value);
        }
    }
}

