namespace Rebus.Tests.Transports.Redis
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
            return new RedisMessageQueue(redisConfiguration, queueName) as IDuplexTransport;
        }
    }
}
