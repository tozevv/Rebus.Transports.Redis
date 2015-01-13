namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Transports.Msmq;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Transactions;

	/// <summary>
	/// Unit tests for Rebus queues / transports
	/// </summary>
    [TestFixture(typeof(RedisMessageQueue))]
    [TestFixture(typeof(MsmqMessageQueue))]
    public class RebusTransportTests<T> where T:IDuplexTransport
    {
		[Test]
		public void WhenSendingMessage_ThenMessageIsDelivered()
		{
			// Arrange
            var queue = GetQueueForTest();
            string message = "aMessage"; 

			// Act
            queue.Send(message);
            string receivedMessage = queue.Receive();

			// Assert
            Assert.AreEqual(message, receivedMessage);
		}

        [Test]
        public void WhenSendingMessageAndExpiring_ThenMessageIsNotDelivered()
        {
            // Arrange
            var queue = GetQueueForTest();
            string message = "aMessage";
            TimeSpan expireIn = TimeSpan.FromSeconds(1); // redis supports min 1 second

            // Act
            queue.Send(message, expireIn);
            Thread.Sleep((int)expireIn.TotalMilliseconds);
            string receivedMessage = queue.Receive();

            // Assert
            Assert.IsNull(receivedMessage);
        }

        [Test]
        public void WhenSendingMessages_ThenMessageOrderIsKept()
        {
            // Arrange
            var queue = GetQueueForTest();
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
            var queue = GetQueueForTest();
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
        public void WhenRollingbackSend_ThenMessageIsDiscarded()
        {
            // Arrange
            var queue = GetQueueForTest();
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
            var queue = GetQueueForTest();
            string message = "aMessage";
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
            var queue = GetQueueForTest();
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

        [Test]
        public void WhenRollingbackReceiveAndSend_ThenMessageIsDiscarded()
        {
            // Arrange
            var queue = GetQueueForTest();
            string firstMessage = "first";
            string secondMessage = "second"; 
            string receivedBeforeRollback = null;
            string receivedAfterRollback = null;

            queue.Send(firstMessage);
            // Act
            using (var transactionScope = new TransactionScope())
            {  
                receivedBeforeRollback = queue.Receive();
                queue.Send(secondMessage);
                transactionScope.Dispose();
            }

            receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(firstMessage, receivedBeforeRollback);
            Assert.AreEqual(firstMessage, receivedAfterRollback);
        }

        [Test]
        public void WhenReceivingAndAbortingTransaction_ThenMessageIsKept()
        {
            // Arrange
            var queue = GetQueueForTest();
            string message = "aMessage";
            string receivedBeforeRollback = null;
            string receivedAfterRollback = null;

            // Act   
            queue.Send(message);

            try
            {
                using (var transactionScope = new TransactionScope())
                {
                    queue.SimulateBrokenTransaction = true;
                    receivedBeforeRollback = queue.Receive();
                }
            }
            catch (TransactionAbortedException) { }

            Thread.Sleep(6000);
            receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(message, receivedBeforeRollback);
            Assert.AreEqual(message, receivedAfterRollback);
        }

        [Test]
        public void WhenReceivingFromTwoConsumersAndRollingbackFirst_ThenSecondConsumerReceivesOutOfOrder()
        {
            // Arrange
            string firstMessage = "first";
            string secondMessage = "second";
            string received1 = null;
            string received2 = null;
            string received3 = null;
            var sendQueue = GetQueueForTest();
            AutoResetEvent consumer2Run = new AutoResetEvent(false);
            AutoResetEvent consumer1Run = new AutoResetEvent(false);

            // Act
            sendQueue.Send(firstMessage);
            sendQueue.Send(secondMessage);

            Thread consumer1 = new Thread(() =>
            {
                using (var transactionScope = new TransactionScope())
                {
                    var queue1 = GetQueueForTest();
                    received1 = queue1.Receive();

                    consumer2Run.Set(); consumer1Run.WaitOne(); // run consumer 2 and wait

                    transactionScope.Dispose(); 
                }
                consumer2Run.Set(); // run consumer 2
            });

            Thread consumer2 = new Thread(() =>
            {
                consumer2Run.WaitOne();
                using (var transactionScope = new TransactionScope())
                {
                    var queue2 = GetQueueForTest();
                    received2 = queue2.Receive();
   
                    consumer1Run.Set(); consumer2Run.WaitOne(); // run consumer 1 and wait

                    received3 = queue2.Receive();
                    transactionScope.Complete();
                }
            });

            consumer1.Start();
            consumer2.Start();
            consumer2.Join(TimeSpan.FromSeconds(5));

            // Assert
            Assert.AreEqual(firstMessage, received1);
            Assert.AreEqual(secondMessage, received2);
            Assert.AreEqual(firstMessage, received3);
        }

        protected virtual IDuplexTransport GetTransport(string queueName)
        {
            if (typeof(T) == typeof(RedisMessageQueue))
            {
                string connectionString = ConfigurationManager.ConnectionStrings["RebusUnitTest"].ConnectionString;
                var redisConfiguration = ConfigurationOptions.Parse(connectionString);
                return new RedisMessageQueue(redisConfiguration, queueName) as IDuplexTransport;
            }
            else if (typeof(T) == typeof(MsmqMessageQueue))
            {
                return new MsmqMessageQueue(queueName) as IDuplexTransport;
            }
            else
            {
                Assert.Fail("Unknown type of duplex transport: ", typeof(T).FullName);
                return null;
            }
        }

        protected virtual SimpleQueue<string> GetQueueForTest([CallerMemberName] string caller = "")
        {
            return new SimpleQueue<string>(GetTransport(caller));
        }
	}
}
