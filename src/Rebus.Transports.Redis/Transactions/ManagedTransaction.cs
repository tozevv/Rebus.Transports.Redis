namespace Rebus.Transports.Redis
{
    using System;
    using StackExchange.Redis;

    public enum TransactionState { Open, Committed, Rolledback }

    public class ManagedTransaction
    {
        private const string transactionCounterKey = "transaction:counter";
        private const string transactionCurrentKey = "transaction:current";

        private readonly IDatabase database;
        private readonly Lazy<ITransaction> commitTransaction;
        private readonly Lazy<ITransaction> rollbackTransaction;
        private readonly string transactionId;

        internal ManagedTransaction(IDatabase database, string transactionId)
        {
            this.database = database;
            this.transactionId = transactionId;

            Func<ITransaction> createInternalTx = () => 
            {
                var tx = database.CreateTransaction();
                tx.AddCondition(Condition.KeyExists(transactionId));
                tx.KeyDeleteAsync(transactionId);
                tx.SortedSetRemoveAsync(transactionCurrentKey, transactionId);
                return tx;
            };

            this.commitTransaction = new Lazy<ITransaction>(createInternalTx);
            this.rollbackTransaction = new Lazy<ITransaction>(createInternalTx);
            this.State = TransactionState.Open;
        }

        public static ManagedTransaction BeginTransaction(IDatabase database) 
        {
            // TODO: get time for redis server
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            DateTime expires = DateTime.UtcNow.Add(timeout);
            
            string transactionId = "transaction:" + database.StringIncrement(transactionCounterKey);

            var batch = database.CreateBatch();
            batch.StringSetAsync(transactionId, transactionId, timeout);
            batch.SortedSetAddAsync(transactionCurrentKey, transactionId, expires.Ticks);
            batch.Execute();

            return new ManagedTransaction(database, transactionId);
        }

        public ITransaction OnCommit 
        { 
            get { return this.commitTransaction.Value; } 
        }

        public ITransaction OnRollback 
        { 
            get { return this.rollbackTransaction.Value; } 
        }

        public IDatabase Immediate
        { 
            get { return this.database; } 
        }

        public string TransactionId 
        {
            get { return this.transactionId; }
        }

        public void Commit() 
        {
            if (this.State != TransactionState.Open)
            {
                throw new InvalidOperationException("Rollback at inconsistent state.");
            }

            if (this.OnCommit.Execute(CommandFlags.PreferMaster))
            {
                State = TransactionState.Rolledback;
            }
            else 
            {
                // most probably timed-out! But not absolutely sure...
                throw new TimeoutException("Transaction timed out.");
            }
        }

        public void Rollback()
        {
            if (this.State != TransactionState.Open)
            {
                throw new InvalidOperationException("Rollback at inconsistent state.");
            }

            if (this.OnRollback.Execute(CommandFlags.PreferMaster))
            {
                State = TransactionState.Rolledback;
            }
            else 
            {
                throw new InvalidOperationException("Rollback failed or already executed.");
            }
        }

        public TransactionState State
        {
            get;
            private set;            
        }
    }
}

