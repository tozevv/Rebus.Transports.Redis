namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Transports.Msmq;
    using Rebus.Transports.Redis;
    using StackExchange.Redis;
    using System;
    using System.Configuration;
    using System.Runtime.CompilerServices;

    /// <summary>
	/// Unit tests for Redis Message Queue.
	/// </summary>
    [TestFixture]
    public class MSMQTests : GenericQueueTests
    {
        public override SimpleQueue<string> GetQueueForTest([CallerMemberName] string caller = "")
        {
            return new SimpleQueue<string>(new MsmqMessageQueue(caller));
        }
    }
}
