using System.Transactions;
using System.Threading;

namespace Rebus.Transports.Redis.Tests
{
    using NUnit.Framework;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    using System;
    using System.Configuration;
    
    [TestFixture(typeof(RedisMessageQueue))]
    public class RedisTransportTests: TransportTestsBase<RedisMessageQueue>
    {
        public RedisTransportTests(Type t) : base() { }

        protected override IDuplexTransport GetTransport(string queueName)
        {
            string connectionString = ConfigurationManager.ConnectionStrings["RebusUnitTest"].ConnectionString;
            var redisConfiguration = ConfigurationOptions.Parse(connectionString);
            redisConfiguration.ResolveDns = true;
            return new RedisMessageQueue(redisConfiguration, queueName) as IDuplexTransport;
        }


        [Test]
        public void WhenDirtyAborting_ThenMessageIsKept()
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

                // force a dirty rollback, eg, without rolling back.
                var transactionContext = queue.GetCurrentTransactionContext();
                var txManager = RedisTransactionManager.Get(transactionContext);
                txManager.AbortWithNoRollback();

                // more than timeout
                Thread.Sleep(10000);

                transactionScope.Dispose();
            }


            receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(message, receivedBeforeRollback);
            Assert.AreEqual(message, receivedAfterRollback);
        }
    }
}
