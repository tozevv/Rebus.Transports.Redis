namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Bus;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    using System.Configuration;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;

	/// <summary>
	/// Unit tests for Redis Message Queue.
	/// </summary>
	[TestFixture]
	public class RedisMessageQueueTests
    {
        private ConfigurationOptions redisConfiguration = null;
        private MessagePacker<string> packer = new MessagePacker<string>();

		[TestFixtureSetUp]
		public void Init()
		{
            string connectionString = ConfigurationManager.ConnectionStrings["RebusUnitTest"].ConnectionString;
            redisConfiguration = ConfigurationOptions.Parse(connectionString);
		}

		[TestFixtureTearDown]
		public void Dispose()
		{
            //var redis = ConnectionMultiplexer.Connect(redisConfiguration);
            //IServer server = redis.GetServer(redis.GetEndPoints()[0]);
            //server.FlushDatabase();
		}

		[Test]
		public void WhenSendingMessage_ThenMessageIsDelivered()
		{
			// Arrange
			string queueName = "WhenSendingMessage_ThenMessageIsDelivered";
			string sentMessage = "message";
			var transactionContext = new NoTransaction();
			var queue = new RedisMessageQueue(redisConfiguration, queueName);

			// Act
			queue.Send(queueName, packer.Pack(sentMessage), transactionContext);
			var receivedMessage = packer.Unpack(queue.ReceiveMessage(transactionContext));

			// Assert
			Assert.AreEqual(sentMessage, receivedMessage);
		}

		[Test]
		public void WhenTransactionCommitted_MessageIsSent()
		{
			// Arrange
			string queueName = "WhenTransactionCommitted_MessageIsSent";
			string sentMessage = "message";
			var queue = new RedisMessageQueue(redisConfiguration, queueName);
			string receivedBeforeCommit = null;
			string receivedAfterCommit = null;

			// Act
			using (var transactionScope = new TransactionScope())
			{
				var transactionContext = 
					new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

				queue.Send(queueName, packer.Pack(sentMessage), transactionContext);
				receivedBeforeCommit = packer.Unpack(queue.ReceiveMessage(transactionContext));
				transactionScope.Complete();
				transactionScope.Dispose();
			}

			using (var transactionScope = new TransactionScope())
			{
				var transactionContext = 
					new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

				receivedAfterCommit = packer.Unpack(queue.ReceiveMessage(transactionContext));
				transactionScope.Complete();
			}

			// Assert
			Assert.IsNull(receivedBeforeCommit);
			Assert.AreEqual(sentMessage, receivedAfterCommit);
		}

		[Test]
		public void WhenTransactionRolledBack_ReceivedMessageIsKept()
		{
			// Arrange
			string queueName = "WhenTransactionRolledBack_ReceivedMessageIsKept";
			string sentMessage = "message";
			var queue = new RedisMessageQueue(redisConfiguration, queueName);
			string receivedMessageBeforeRollback = null;
			string receivedMessageAfterRollback = null;

			// Act
			using (var transactionScope = new TransactionScope())
			{
				var transactionContext = 
					new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction
                
				queue.Send(queueName, packer.Pack(sentMessage), 
					new NoTransaction()); // simulate other transaction sent the message before
				receivedMessageBeforeRollback = packer.Unpack(queue.ReceiveMessage(transactionContext));

				// rollback
			}
			using (var transactionScope = new TransactionScope())
			{
				var transactionContext = 
					new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

				receivedMessageAfterRollback = packer.Unpack(queue.ReceiveMessage(transactionContext));
				transactionScope.Complete();
			}

			// Assert
			Assert.AreEqual(sentMessage, receivedMessageBeforeRollback, "Receive message failed");
			Assert.AreEqual(sentMessage, receivedMessageAfterRollback, "Receive message rollback failed");
		}

		[Test]
		public void WhenTransactionRolledBack_ReceivedMessageOrderIsKept()
		{
			// Arrange
			string queueName = "WhenTransactionRolledBack_ReceivedMessageOrderIsKept";
			string[] sentMessages = new string[] { "message1", "message2", "message3" };
			string[] receivedMessagesBeforeRollback = new string[sentMessages.Length];
			string[] receivedMessagesAfterRollback = new string[sentMessages.Length];

			var transactionScope = new TransactionScope();
			var transactionContext = 
				new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction
			var queue = new RedisMessageQueue(redisConfiguration, queueName);

			// Act
			foreach (var sentMessage in sentMessages)
			{
				queue.Send(queueName, packer.Pack(sentMessage), 
					new NoTransaction()); // simulate other transaction sent the message before
			}

			for (int i = 0; i < sentMessages.Length; i++)
			{
				receivedMessagesBeforeRollback[i] = packer.Unpack(queue.ReceiveMessage(transactionContext));
			}

			transactionScope.Dispose(); // rollback
			transactionScope = new TransactionScope();

			for (int i = 0; i < sentMessages.Length; i++)
			{
                receivedMessagesAfterRollback[i] = packer.Unpack(queue.ReceiveMessage(transactionContext));
			}

			transactionScope.Complete();
			transactionScope.Dispose();

			// Assert
			CollectionAssert.AreEqual(sentMessages, receivedMessagesBeforeRollback, "Receive message failed");
			CollectionAssert.AreEqual(sentMessages, receivedMessagesAfterRollback, "Receive message rollback order failed");
		}

		[Test]
		public void WhenTransactionTimesOut_ReceivedMessageIsKept()
		{
			// Arrange
			string queueName = "WhenTransactionTimesOut_ReceivedMessageIsKept";
			string sentMessage = "message";
			var transactionScope = new TransactionScope();
			var transactionContext = 
				new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction
			var queue = new RedisMessageQueue(redisConfiguration, queueName);

			// Act
			queue.Send(queueName, packer.Pack(sentMessage), 
				new NoTransaction()); // simulate other transaction sent the message before
			var receivedMessageBeforeRollback = packer.Unpack(queue.ReceiveMessage(transactionContext));
			transactionScope.Dispose();
            var receivedMessageAfterRollback = packer.Unpack(queue.ReceiveMessage(transactionContext));

			// Assert
			Assert.AreEqual(sentMessage, receivedMessageBeforeRollback, "Receive message failed");
			Assert.AreEqual(sentMessage, receivedMessageAfterRollback, "Receive message rollback failed");
		}

		[Test]
		public void SendAndReceiveNoTransactionThroughput()
		{
			// Arrange
			string queueName = "Throughput";
			var queue = new RedisMessageQueue(redisConfiguration, queueName);
			Stopwatch swSend = new Stopwatch();
			Stopwatch swReceive = new Stopwatch();
			long sentMessages = 0;
			long receivedMessages = 0;
			var transactionContext = new NoTransaction();
			var message = packer.Pack("simple message");

			// Act
			swSend.Start();
			Parallel.For(0, 5, new ParallelOptions { MaxDegreeOfParallelism = 50 }, (j) =>
				{
					for (int i = 0; i < 10; i++)
					{
						queue.Send(queueName, message, transactionContext);
						Interlocked.Increment(ref sentMessages);
					}
				});

			swSend.Stop();

			swReceive.Start();
			Parallel.For(0, 5, new ParallelOptions { MaxDegreeOfParallelism = 50 }, (j) =>
				{
					for (int i = 0; i < 10; i++)
					{
						var msg = queue.ReceiveMessage(transactionContext);
						Assert.AreEqual("simple message", packer.Unpack(msg));
						Interlocked.Increment(ref receivedMessages);
					}
				});
			swReceive.Stop();

			Assert.AreEqual(sentMessages, receivedMessages, "Expect to receive the same number of messages"); 
			Assert.Pass(string.Format("Send Throughput of {0} messages / sec\nReceive Throughput of {1} messages / sec", 
					sentMessages / swSend.Elapsed.TotalSeconds,
					receivedMessages / swReceive.Elapsed.TotalSeconds));
		}

		[Test]
		public void SendWithTransactionThroughput()
		{
			// Arrange
			string queueName = "Throughput";
			var queue = new RedisMessageQueue(redisConfiguration, queueName);
			Stopwatch sw = new Stopwatch();
			long sentMessages = 0;
			var message = packer.Pack("simple message");

			// Act
			sw.Start();
			Parallel.For(0, 5, (j) =>
				{
					var transactionScope = new TransactionScope();
					var transactionContext = 
						new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

					for (int i = 0; i < 20; i++)
					{
						queue.Send(queueName, message, transactionContext);
						Interlocked.Increment(ref sentMessages);
					}

					transactionScope.Complete();
				});
			sw.Stop();

			Assert.Pass(string.Format("Throughput of {0} messages / sec", sentMessages / sw.Elapsed.TotalSeconds));
		}
	}
}
