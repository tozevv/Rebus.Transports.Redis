namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Bus;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    using System;
    using System.Collections.Generic;
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
			string message = "aMessage"; 
            var queue = new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, "WhenSendingMessage_ThenMessageIsDelivered"));

			// Act
            queue.Send(message);
            string receivedMessage = queue.Receive();

			// Assert
            Assert.AreEqual(message, receivedMessage);
		}

        [Test]
        public void WhenSendingMessages_ThenOrderIsKept()
        {
            // Arrange
            var queue = new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, "WhenSendingMessages_ThenOrderIsKept"));
            IEnumerable<string> messages = new List<string> { "msg1", "msg2", "msg3" };
           
            // Act
            queue.SendAll(messages);
            var receivedMessages = queue.ReceiveAll();

            // Assert
            Assert.AreEqual(messages, receivedMessages);
        }

        [Test]
        public void WhenSendingMessagesInTransaction_ThenMessageIsDeliveredWithCommit()
        {
            // Arrange
            var queue = new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, "WhenSendingMessagesInTransaction_ThenMessageIsDeliveredWithCommit"));
            string message = "aMessage";
            string receivedBeforeCommit = null;
            string receivedAfterCommit = null;

            // Act
            using (var transactionScope = new TransactionScope())
            {
                queue.Send(message);
                receivedBeforeCommit = queue.Receive();
                transactionScope.Complete();
            }

            receivedAfterCommit = queue.Receive();

            // Assert
            Assert.IsNull(receivedBeforeCommit, "Message was not sent prior to commit.");
            Assert.AreEqual(message, receivedAfterCommit);
        }

        [Test]
        public void WhenRollingbackSend_ThenMessageIsNotSent()
        {
            // Arrange
            var queue = new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, "WhenRollingbackSend_ThenMessageIsNotSent"));
            string message = "aMessage";

            // Act
            using (var transactionScope = new TransactionScope())
            {
                queue.Send(message);
                transactionScope.Dispose();
            }

            string receivedAfterRollback = queue.Receive();

            // Assert
            Assert.IsNull(receivedAfterRollback);
        }
        
        [Test]
        public void WhenRollingbackReceive_ThenMessageIsKept()
        {
             // Arrange
            string message = "aMessage";
            var queue = new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, "WhenRollingbackReceive_ThenMessageIsKept"));
            string receivedBeforeRollback = null;
            string receivedAfterRollback = null;

            // Act
            queue.Send(message);

            using (var transactionScope = new TransactionScope())
            {
                receivedBeforeRollback = queue.Receive();
                transactionScope.Dispose();
            }

            receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(message, receivedBeforeRollback);
            Assert.AreEqual(message, receivedAfterRollback);
        }

        [Test]
        public void WhenRollingbackReceive_ThenMessageOrderIsKept()
        {
            // Arrange
            var queue = new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, "WhenRollingbackReceive_ThenMessageOrderIsKept"));
            List<string> messages = new List<string> { "msg1", "msg2", "msg3" };
            IEnumerable<string> receivedBeforeRollback = null;
            IEnumerable<string> receivedAfterRollback = null;
         
            // Act
            queue.SendAll(messages);

            using (var transactionScope = new TransactionScope())
            {
                receivedBeforeRollback = queue.ReceiveAll();
                transactionScope.Dispose();
            }

            receivedAfterRollback = queue.ReceiveAll();

            // Assert
            Assert.AreEqual(messages, receivedBeforeRollback);
            Assert.AreEqual(messages, receivedAfterRollback);
        }
	
        // TODO: add timeout test
	}
}
