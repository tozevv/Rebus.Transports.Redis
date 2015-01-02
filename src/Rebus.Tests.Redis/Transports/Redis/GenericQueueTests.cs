namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Transactions;

	/// <summary>
	/// Unit tests for Rebus queues
	/// </summary>
	public abstract class GenericQueueTests 
    {
        public abstract SimpleQueue<string> GetQueueForTest([CallerMemberName] string caller = "");
        
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
        public void WhenSendingMessages_ThenOrderIsKept()
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

            Thread.Sleep(1000);
            receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(message, receivedBeforeRollback);
            Assert.AreEqual(message, receivedAfterRollback);
        }
	}
}
