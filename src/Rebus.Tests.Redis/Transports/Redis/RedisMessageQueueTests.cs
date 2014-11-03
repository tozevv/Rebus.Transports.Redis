using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;

namespace Rebus.Tests.Transports.Redis
{
    using System.Text;

    using NUnit.Framework;
    using Rebus.Transports.Redis;
    using Rebus.Bus;
    using StackExchange.Redis;
    using System.Transactions;

    /// <summary>
    /// Unit tests for Redis Message Queue.
    /// </summary>
    [TestFixture]
    public class RedisMessageQueueTests
    {
        private RedisServer server = null;

        [TestFixtureSetUp]
        public void Init()
        {
			server = new RedisServer(6666);
            server.Start();
        }

        [TestFixtureTearDown]
        public void Dispose()
        {
            server.Stop();
        }

        [Test]
        public void WhenSendingMessage_ThenMessageIsDelivered()
        {
			// Arrange
            string queueName = "WhenSendingMessage_ThenMessageIsDelivered";
			string sentMessage = "message";
            var transactionContext = new NoTransaction();
			var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);

			// Act
            queue.Send(queueName, CreateStringMessage(sentMessage), transactionContext);
            var receivedMessage = GetStringMessage(queue.ReceiveMessage(transactionContext));

			// Assert
            Assert.AreEqual(sentMessage, receivedMessage);
        }

        [Test]
        public void WhenTransactionCommitted_MessageIsSent()
        {
			// Arrange
            string queueName = "WhenTransactionCommitted_MessageIsSent";
			string sentMessage = "message";
            var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);
            string receivedBeforeCommit = null;
            string receivedAfterCommit = null;

			// Act
            using (var transactionScope = new TransactionScope())
            {
                var transactionContext = 
                    new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

                queue.Send(queueName, CreateStringMessage(sentMessage), transactionContext);
                receivedBeforeCommit = GetStringMessage(queue.ReceiveMessage(transactionContext));
                transactionScope.Complete();
                transactionScope.Dispose();
            }

            using (var transactionScope = new TransactionScope())
            {
                var transactionContext = 
                    new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

                receivedAfterCommit = GetStringMessage(queue.ReceiveMessage(transactionContext));
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
			var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);
            string receivedMessageBeforeRollback = null;
            string receivedMessageAfterRollback = null;

			// Act
            using (var transactionScope = new TransactionScope())
            {
                var transactionContext = 
                    new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

                queue.Send(queueName, CreateStringMessage(sentMessage), 
                    new NoTransaction()); // simulate other transaction sent the message before
                receivedMessageBeforeRollback = GetStringMessage(queue.ReceiveMessage(transactionContext));

                // rollback
            }
            using (var transactionScope = new TransactionScope())
            {
                var transactionContext = 
                    new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

                receivedMessageAfterRollback = GetStringMessage(queue.ReceiveMessage(transactionContext));
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
			var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);

			// Act
			foreach (var sentMessage in sentMessages) 
			{
				queue.Send(queueName, CreateStringMessage(sentMessage), 
					new NoTransaction()); // simulate other transaction sent the message before
			}

			for (int i = 0; i < sentMessages.Length; i++) 
			{
				receivedMessagesBeforeRollback[i] = GetStringMessage(queue.ReceiveMessage(transactionContext));
			}

			transactionScope.Dispose(); // rollback
			transactionScope = new TransactionScope();

			for (int i = 0; i < sentMessages.Length; i++) 
			{
				receivedMessagesAfterRollback[i] = GetStringMessage(queue.ReceiveMessage(transactionContext));
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
			var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);

			// Act
			queue.Send(queueName, CreateStringMessage(sentMessage), 
				new NoTransaction()); // simulate other transaction sent the message before
			var receivedMessageBeforeRollback = GetStringMessage(queue.ReceiveMessage(transactionContext));
			transactionScope.Dispose();
			var receivedMessageAfterRollback = GetStringMessage(queue.ReceiveMessage(transactionContext));

			// Assert
			Assert.AreEqual(sentMessage, receivedMessageBeforeRollback, "Receive message failed");
			Assert.AreEqual(sentMessage, receivedMessageAfterRollback, "Receive message rollback failed");
		}

        [Test]
        public void SendNoTransactionThroughput()
        {
            // Arrange
            string queueName = "Throughput";
            var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);
            Stopwatch sw = new Stopwatch();
            long sentMessages = 0;

            // Act
            var transactionContext = new NoTransaction();
            var message = CreateStringMessage("simple message");

            sw.Start();
            Parallel.For(0, 50, (j) =>
                {
                    for (int i = 0; i < 200; i++)
                    {
                        queue.Send(queueName, message, transactionContext);
                        Interlocked.Increment(ref sentMessages);
                    }
                });
            sw.Stop();

            Assert.Pass(string.Format("Throughput of {0} messages / sec", sentMessages / sw.Elapsed.TotalSeconds));
        }

        [Test]
        public void SendWithTransactionThroughput()
        {
            // Arrange
            string queueName = "Throughput";
            var queue = new RedisMessageQueue(server.ClientConfiguration, queueName);
            Stopwatch sw = new Stopwatch();
            long sentMessages = 0;
           
            // Act
            var message = CreateStringMessage("simple message");

            sw.Start();
            Parallel.For(0, 50, (j) =>
                {
                    var transactionScope = new TransactionScope();
                    var transactionContext = 
                        new Rebus.Bus.AmbientTransactionContext(); // enlists in ambient transaction

                    for (int i = 0; i < 200; i++)
                    {
                        queue.Send(queueName, message, transactionContext);
                        Interlocked.Increment(ref sentMessages);
                    }

                    transactionScope.Complete();
                });
            sw.Stop();

            Assert.Pass(string.Format("Throughput of {0} messages / sec", sentMessages / sw.Elapsed.TotalSeconds));
        }

        protected TransportMessageToSend CreateStringMessage(string contents)
        {
            return
                new TransportMessageToSend
            {
                Body = Encoding.UTF8.GetBytes(contents),
                Label = typeof(string).FullName
            };
        }

        protected string GetStringMessage(ReceivedTransportMessage message)
        {
            return message == null ? null :
                Encoding.UTF8.GetString(message.Body);
        }
    }
}
