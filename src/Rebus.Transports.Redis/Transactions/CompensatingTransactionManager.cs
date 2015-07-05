using System;
using StackExchange.Redis;

namespace Rebus.Transports.Redis
{
    public class CompensatingTransactionManager
    {
        private readonly IDatabase database;

        public CompensatingTransactionManager(IDatabase database)
        {
            this.database = database;
        }

        public CompensatingTransaction BeginTransaction() 
        {
            string transactionLog = 
                (string)this.database.ScriptEvaluate(@"
                    local transactionLog = 'transaction:' .. redis.call('INCR', 'transaction:counter')

                    local transactionTimeout = redis.call('GET', 'transaction:timeout')
                    if  (transactionTimeout == false) then
                        transactionTimeout = 60
                    end
     
                    local transactionLock = transactionLog .. ':lock'
                    redis.call('SETEX', transactionLock, transactionTimeout, '1')

                    return transactionLog
                    ");
            ITransaction commitTransaction = this.database.CreateTransaction();
            return new CompensatingTransaction(database, commitTransaction, transactionLog);
        }

        public void RollbackTimeoutTransactions() 
        {
            return;   
        }
    }
}

