namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Transactions;

	/// <summary>
	/// Unit tests for Rebus queues
	/// </summary>
	public abstract class QueueTests 
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

        [Test]
        public void WhenReceivingAndRollingback_ThenOutOfOrder()
        {
            // Arrange
            string firstMessage = "first";
            string secondMessage = "second";
            string received1 = null;
            string received2 = null;
            string received3 = null;
            var sendQueue = GetQueueForTest();
            AutoResetEvent thread2Run = new AutoResetEvent(false);
            AutoResetEvent thread1Run = new AutoResetEvent(false);

            // Act
            sendQueue.Send(firstMessage);
            sendQueue.Send(secondMessage); 

            Thread thread1 = new Thread(() =>
            {
                using (var transactionScope = new TransactionScope())
                {
                    var queue1 = GetQueueForTest();
                    received1 = queue1.Receive();

                    thread2Run.Set(); thread1Run.WaitOne(); // run thread 2 and wait

                    transactionScope.Dispose(); 
                }
                thread2Run.Set(); // run thread 2
            });

            Thread thread2 = new Thread(() =>
            {
                thread2Run.WaitOne();
                using (var transactionScope = new TransactionScope())
                {
                    var queue2 = GetQueueForTest();
                    received2 = queue2.Receive();
   
                    thread1Run.Set(); thread2Run.WaitOne(); // run thread 1 and wait

                    received3 = queue2.Receive();
                    transactionScope.Complete();
                }
            });

            thread1.Start();
            thread2.Start();
            thread2.Join(TimeSpan.FromSeconds(5));

            // Assert
            Assert.AreEqual(firstMessage, received1);
            Assert.AreEqual(secondMessage, received2);
            Assert.AreEqual(firstMessage, received3);
        }
	}
}
