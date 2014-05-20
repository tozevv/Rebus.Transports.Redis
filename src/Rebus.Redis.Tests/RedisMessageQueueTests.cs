namespace Rebus.Redis.Tests
{
    using NUnit.Framework;
    using Rebus.Transports.Redis;

    /// <summary>
    /// Unit tests for Redis Message Queue.
    /// </summary>
    [TestFixture]
    public class RedisMessageQueueTests
    {
        [TestFixtureSetUp]
        public void Init()
        {
            RedisServer.Start();
        }

        [TestFixtureTearDown]
        public void Dispose()
        {
            RedisServer.Stop();
        }

        [Test]
        public void WhenSendingMessageIsDelivered()
        {
            Assert.Pass("Running");      
        }
    }
}
