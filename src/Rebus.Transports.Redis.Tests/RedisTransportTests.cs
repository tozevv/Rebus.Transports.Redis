namespace Rebus.Transports.Redis.Tests
{
    using System;
    using System.Configuration;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using NUnit.Framework;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    
    [TestFixture(typeof(RedisMessageQueue))]
    public class RedisTransportTests: TransportTestsBase<RedisMessageQueue>
    {
        public RedisTransportTests(Type t) : base() { }
 
        [Test]
        [Category("Transaction")]
        public void WhenDirtyAborting_ThenMessageIsKept()
        {
            // Arrange
            var queue = GetQueueForTest();
            string message = "aMessage";
            string receivedBeforeRollback = null;
            string receivedAfterTimeout = null;

            // Act
            queue.Send(message);
         
            Task.Factory.StartNew(async () =>
                {
                    using (var transactionScope = new TransactionScope())
                    {
                        receivedBeforeRollback = queue.Receive();

                        // timeout
                        await Task.Delay(TimeSpan.FromSeconds(5));

                        transactionScope.Dispose();
                    }
                });

            Task.Factory.StartNew(async () =>
                {
                    using (var transactionScope = new TransactionScope())
                    {
                        // timeout
                        await Task.Delay(TimeSpan.FromSeconds(4));

                        receivedAfterTimeout = queue.Receive();
                    }
                });
          
            //receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(message, receivedBeforeRollback);
            Assert.AreEqual(message, receivedAfterTimeout);
        }

        protected override IDuplexTransport GetTransport(string queueName)
        {
            return new RedisMessageQueue(GetRedisConfig(), queueName) as IDuplexTransport;
        }

        private ConfigurationOptions GetRedisConfig()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["RebusUnitTest"].ConnectionString;
            var redisConfiguration = ConfigurationOptions.Parse(connectionString);
            redisConfiguration.ResolveDns = true;
            return redisConfiguration;
        }
    }
}
