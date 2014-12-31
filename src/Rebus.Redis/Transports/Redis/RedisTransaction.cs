namespace Rebus.Transports.Redis
{
    using System;
    using System.Linq;
    using StackExchange.Redis;
    using System.Collections.Generic;

    internal class RedisTransaction
    {
        private readonly IDatabase db;
        private readonly long transactionId;
        private readonly Lazy<ITransaction> commitTx;
        private readonly Lazy<ITransaction> rollbackTx;

        private const string TransactionCounterKey = "rebus:transaction:counter";
        private const string TransactionRunningKey = "rebus:transaction:running";
        private const string TransactionLockKey = "rebus:transaction:{0}";

        public RedisTransaction(IDatabase db, TimeSpan timeout)
        {
            this.db = db;
            long transactionId = db.StringIncrement(TransactionCounterKey);
            this.transactionId = transactionId;
            db.StringSet(string.Format(TransactionLockKey, transactionId), transactionId, timeout, When.Always);
            db.SetAdd(TransactionRunningKey, transactionId);
            commitTx = new Lazy<ITransaction>(() => this.db.CreateTransaction());
            rollbackTx = new Lazy<ITransaction>(() => this.db.CreateTransaction());
        }

        public long TransactionId
        { 
            get
            {
                return transactionId;
            } 
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

        public void Commit()
        {
            this.CommitTx.KeyDeleteAsync(string.Format(TransactionLockKey, transactionId));
            this.CommitTx.Execute();
        }

        public void Rollback()
        {
            this.RollbackTx.KeyDeleteAsync(string.Format(TransactionLockKey, transactionId));
            this.RollbackTx.Execute();
        }

        public static bool IsTransactionActive(IDatabase database, long transactionId)
        {
            return database.KeyExists(string.Format(TransactionLockKey, transactionId));
        }
    }
}

