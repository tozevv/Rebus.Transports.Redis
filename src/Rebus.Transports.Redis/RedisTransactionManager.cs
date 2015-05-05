namespace Rebus.Transports.Redis
{
    using System;
    using System.Linq;
    using StackExchange.Redis;
    using System.Collections.Generic;

    /// <summary>
    /// Manages redis transactions keeping one transaction open for commits and o ther for
    /// </summary>
    internal class RedisTransactionManager
    {
        private const string TransactionCounterKey = "rebus:transaction:counter";
        private const string TransactionLockKey = "rebus:transaction:{0}";
        private const string RedisContextKey = "redis:context";

        private readonly IDatabase db;
        private readonly Lazy<ITransaction> commitTx;
        private readonly Lazy<ITransaction> rollbackTx;
        private readonly ITransactionContext context;
        private readonly TimeSpan timeout;
        private bool dirtyAbort = false;
     
        /// <summary>
        /// Initializes a new instance of the <see cref="Rebus.Transports.Redis.RedisTransactionManager"/> class.
        /// </summary>
        /// <param name="context">Rebus transaction Context.</param>
        /// <param name="db">Rebus database instance.</param>
        /// <param name="timeout">Transaction timeout.</param>
        public RedisTransactionManager(ITransactionContext context, IDatabase db, TimeSpan timeout)
        {
            this.db = db;
            this.context = context;
            this.timeout = timeout;
            commitTx = new Lazy<ITransaction>(() => this.db.CreateTransaction());
            rollbackTx = new Lazy<ITransaction>(() => this.db.CreateTransaction());
        }

        /// <summary>
        /// Begins a transaction.
        /// </summary>
        public void BeginTransaction()
        {
            this.TransactionId = db.StringIncrement(TransactionCounterKey);
            db.StringSet(string.Format(TransactionLockKey, this.TransactionId), this.TransactionId, timeout, When.Always);
     
            context.DoCommit += () =>
			{
                if (dirtyAbort)
                {
                    return;
                }
                this.CommitTx.KeyDeleteAsync(string.Format(TransactionLockKey, this.TransactionId));
                this.CommitTx.Execute();
			};

			context.DoRollback += () =>
			{
                if (dirtyAbort)
                {
                    return;
                }
                this.RollbackTx.KeyDeleteAsync(string.Format(TransactionLockKey, this.TransactionId));
                this.RollbackTx.Execute();
			};

        }
      
        /// <summary>
        /// Gets the transaction identifier.
        /// </summary>
        /// <value>The transaction identifier.</value>
        public long TransactionId
        {
            get;
            private set;
        }

        /// <summary>
        /// Get the redis transaction used for commit a rebus transaction.
        /// </summary>
        /// <value>The commit tx.</value>
        public ITransaction CommitTx
        {
            get
            { 
                return commitTx.Value;
            }
        }

        /// <summary>
        /// Get the redis transaction used for rollback a rebus transaction.
        /// </summary>
        /// <value>The commit tx.</value>
        public ITransaction RollbackTx
        {
            get
            { 
                return rollbackTx.Value;
            }
        }

        /// <summary>
        /// Aborts the with no rollback simulating a crashing or network partition.
        /// This is only usefull to test.
        /// </summary>
        public void AbortWithNoRollback() 
        {
            this.dirtyAbort = true;
        }

        /// <summary>
        /// Determines if a transaction is active for the specified database and transactionId.
        /// </summary>
        /// <returns><c>true</c> if the provided transaction is active; otherwise, <c>false</c>.</returns>
        /// <param name="database">Redis database instance.</param>
        /// <param name="transactionId">Transaction identifier.</param>
        public static bool IsTransactionActive(IDatabase database, long transactionId)
        {
            return database.KeyExists(string.Format(TransactionLockKey, transactionId));
        }
       
        /// <summary>
        /// Get the transaction manager for the Rebus transaction context.
        /// </summary>
        /// <param name="context">Rebus transaction context.</param>
        /// <returns>The active RedisTransactionManager or null if none found.</returns>
        public static RedisTransactionManager Get(ITransactionContext context)
        {
            // locking not needed here 
            // assuming 1-to-1 relathionship between current worker and context
            var redisTransaction = context[RedisContextKey] as RedisTransactionManager;
            return redisTransaction;
        }
            
        /// <summary>
        /// Gets or creates the transaction manager for the Rebus transaction context.
        /// </summary>
        /// <returns>The or create.</returns>
        /// <param name="context">Rebus transaction context.</param>
        /// <param name="database">Redis database instance.</param>
        /// <param name="transactionTimeout">Transaction timeout.</param>
        /// <returns>The active RedisTransactionManager.</returns>
        public static RedisTransactionManager GetOrCreate(ITransactionContext context, IDatabase database, TimeSpan transactionTimeout)
        {
            if (! context.IsTransactional)
            {
                return null;
            }
            // locking not needed here 
            // assuming 1-to-1 relathionship between current worker and context
            var redisTransaction = context[RedisContextKey] as RedisTransactionManager;
            if (redisTransaction == null)
            {
                redisTransaction = new RedisTransactionManager(context, database, transactionTimeout);
                redisTransaction.BeginTransaction();

                context[RedisContextKey] = redisTransaction;
            }
            return redisTransaction;
        }
    }
}

