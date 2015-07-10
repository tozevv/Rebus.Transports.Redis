using System;
using StackExchange.Redis;

namespace Rebus.Transports.Redis
{
    public static class IDatabaseExtensions
    {
        public static CompensatingTransaction BeginCompensatingTransaction(this IDatabase database)
        {
            return CompensatingTransaction.BeginTransaction(database);
        }
    }
}

