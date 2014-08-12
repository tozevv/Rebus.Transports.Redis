using System;
using System.Linq;
using StackExchange.Redis;

namespace Rebus.Transports.Redis
{
	internal class RedisTransactionContext
	{
		private readonly IDatabase db;
		private readonly long transactionId = 0;
		private readonly Lazy<ITransaction> lazyTransaction;

		private const string RedisContextKey = "redis_context";
		private const string TransactionCounterKey = "rebus:transaction:counter";
		private const string TransactionLockKey = "rebus:transaction:{0}";
	
		public static RedisTransactionContext GetFromTransactionContext(IDatabase db, ITransactionContext context)
		{
			if (context.IsTransactional)
			{
				// locking not needed here 
				// assuming 1-to-1 relathionship between current worker and context
				var redisTransactionContext = context[RedisContextKey] as RedisTransactionContext;
				if (redisTransactionContext == null)
				{
					long transactionId = db.StringIncrement(TransactionCounterKey);
					redisTransactionContext = new RedisTransactionContext(db, transactionId);
					redisTransactionContext.SetTimeout(TimeSpan.FromMinutes(1));

					context[RedisContextKey] = redisTransactionContext;

					context.DoCommit += () =>
					{
						redisTransactionContext.Commit();
					};

					context.DoRollback += () =>
					{
						redisTransactionContext.Rollback();
					};
				}
				return redisTransactionContext;
			}
			else
			{
				return new RedisTransactionContext(db, 0);
			}
		}

		public static bool IsTransactionActive(IDatabase database, long transactionId)
		{
			return database.KeyExists(string.Format(TransactionLockKey, transactionId));
		}

		public long TransactionId
		{ 
			get
			{
				return transactionId;
			} 
		}

		public ITransaction Transaction
		{
			get
			{ 
				return lazyTransaction.Value;
			}
		}

		public void Commit()
		{
            Transaction.KeyDeleteAsync(string.Format(TransactionLockKey, transactionId));
			Transaction.Execute();
		}

		public void Rollback()
		{

		}

		public void SetTimeout(TimeSpan timeout)
		{
			db.StringSet(string.Format(TransactionLockKey, transactionId), transactionId, timeout, When.Always);
		}
			
		private RedisTransactionContext(IDatabase db, long transactionId)
		{
			this.db = db;
			this.transactionId = transactionId;
			lazyTransaction = new Lazy<ITransaction>(() => this.db.CreateTransaction());
		}

	}
}

