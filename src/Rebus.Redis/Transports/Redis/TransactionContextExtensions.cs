namespace Rebus.Transports.Redis
{
    using StackExchange.Redis;
    using System;

    internal static class TransactionContextExtensions
    {
        private const string RedisContextKey = "redis:context";

        public static RedisTransactionManager GetTransactionManager(this ITransactionContext context, IDatabase database)
        {
			// locking not needed here 
			// assuming 1-to-1 relathionship between current worker and context
			var redisTransaction = context[RedisContextKey] as RedisTransactionManager;
			if (redisTransaction == null)
			{
				redisTransaction = new RedisTransactionManager(context, database, TimeSpan.FromMinutes(1));
                if (context.IsTransactional)
                {
                    redisTransaction.BeginTransaction();
                }
				context[RedisContextKey] = redisTransaction;
			}
			return redisTransaction;
        }
    }
}
