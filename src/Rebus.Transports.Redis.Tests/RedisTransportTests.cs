namespace Rebus.Transports.Redis.Tests
{
    using System;
    using System.Configuration;
    using System.Threading;
    using System.Transactions;
    using NUnit.Framework;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    
    [TestFixture(typeof(RedisMessageQueue))]
    public class RedisTransportTests: TransportTestsBase<RedisMessageQueue>
    {
        private TimeSpan transactionTimeout = TimeSpan.FromSeconds(1);
        public RedisTransportTests(Type t) : base() { }
 
            
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
            Console.WriteLine("WhenDirtyAborting_ThenMessageIsKept");

            using (var transactionScope = new TransactionScope())
            {
                receivedBeforeRollback = queue.Receive();
                DumpRedisKeys();

                // force a dirty rollback, eg, without rolling back.
                var transactionContext = queue.GetCurrentTransactionContext();
                var txManager = RedisTransactionManager.Get(transactionContext);
                txManager.AbortWithNoRollback();

                DumpRedisKeys();

                // more than timeout
                Thread.Sleep(transactionTimeout.Add(TimeSpan.FromSeconds(2)));
           
                transactionScope.Dispose();
            }
            DumpRedisKeys();

            receivedAfterRollback = queue.Receive();

            // Assert
            Assert.AreEqual(message, receivedBeforeRollback);
            Assert.AreEqual(message, receivedAfterRollback);
        }

        protected override IDuplexTransport GetTransport(string queueName)
        {
            return new RedisMessageQueue(GetRedisConfig(), queueName, transactionTimeout) as IDuplexTransport;
        }

        private ConfigurationOptions GetRedisConfig()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["RebusUnitTest"].ConnectionString;
            var redisConfiguration = ConfigurationOptions.Parse(connectionString);
            redisConfiguration.ResolveDns = true;
            return redisConfiguration;
        }

        private void DumpRedisKeys()
        {
            var redis =  ConnectionMultiplexer.Connect(GetRedisConfig());
            foreach (var key in redis.GetServer(redis.GetEndPoints()[0]).Keys())
            {
                Console.WriteLine("Redis key: " + key);
            }
        }
    }
}
