using System;
using StackExchange.Redis;

namespace Rebus.Transports.Redis
{
    public static class IDatabaseExtensions
    {
        public static RedisCompensatingTransaction BeginCompensatingTransaction(this IDatabase database)
        {
            return new RedisCompensatingTransaction(database);
        }
    }
}

