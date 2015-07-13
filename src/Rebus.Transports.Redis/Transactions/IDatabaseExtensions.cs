namespace Rebus.Transports.Redis.Transactions
{
    using StackExchange.Redis;

    public static class IDatabaseExtensions
    {
        /// <summary>
        /// Create a new complex redis pseudo-transaction.
        /// </summary>
        /// <returns>The transaction.</returns>
        /// <param name="database">Database.</param>
        public static ManagedTransaction BeginManagedTransaction(this IDatabase database)
        {
            return ManagedTransaction.BeginTransaction(database);
        }

    }
}

