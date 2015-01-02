namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    using System;
    using System.Configuration;
    using System.Runtime.CompilerServices;

    /// <summary>
	/// Unit tests for Redis Message Queue.
	/// </summary>
    [TestFixture]
    public class RedisMessageQueueTests : GenericQueueTests
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

        public override SimpleQueue<string> GetQueueForTest([CallerMemberName] string caller = "")
        {
            return new SimpleQueue<string>(new RedisMessageQueue(redisConfiguration, caller));
        }
    }
}
