namespace Rebus.Transports.Redis
{
    using System;
    using System.Linq;
    using StackExchange.Redis;
    using System.Collections.Generic;

    internal class RedisTransactionManager
    {
        private const string TransactionCounterKey = "rebus:transaction:counter";
        private const string TransactionLockKey = "rebus:transaction:{0}";

        private readonly IDatabase db;
        private readonly Lazy<ITransaction> commitTx;
        private readonly Lazy<ITransaction> rollbackTx;
        private readonly ITransactionContext context;
        private readonly TimeSpan timeout;
     
        public RedisTransactionManager(ITransactionContext context, IDatabase db, TimeSpan timeout)
        {
            this.db = db;
            this.context = context;
            this.timeout = timeout;
            commitTx = new Lazy<ITransaction>(() => this.db.CreateTransaction());
            rollbackTx = new Lazy<ITransaction>(() => this.db.CreateTransaction());
        }

        public void BeginTransaction()
        {
            this.TransactionId = db.StringIncrement(TransactionCounterKey);
            db.StringSet(string.Format(TransactionLockKey, this.TransactionId), this.TransactionId, timeout, When.Always);
     
            context.DoCommit += () =>
			{
                this.CommitTx.KeyDeleteAsync(string.Format(TransactionLockKey, this.TransactionId));
                this.CommitTx.Execute();
			};

			context.DoRollback += () =>
			{
                this.RollbackTx.KeyDeleteAsync(string.Format(TransactionLockKey, this.TransactionId));
                this.RollbackTx.Execute();
			};

        }
      
        public long TransactionId
        {
            get;
            private set;
        }

        public ITransaction CommitTx
        {
            get
            { 
                return commitTx.Value;
            }
        }

        public ITransaction RollbackTx
        {
            get
            { 
                return rollbackTx.Value;
            }
        }

        public static bool IsTransactionActive(IDatabase database, long transactionId)
        {
            return database.KeyExists(string.Format(TransactionLockKey, transactionId));
        }
    }
}

