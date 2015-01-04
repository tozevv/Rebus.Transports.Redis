namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Transports.Msmq;
    using System.Runtime.CompilerServices;

    /// <summary>
	/// Unit tests for Redis Message Queue.
	/// </summary>
    [TestFixture("baseline")]
    public class MSMQTests : QueueTests
    {
        public MSMQTests(string ignore) { }

        public override SimpleQueue<string> GetQueueForTest([CallerMemberName] string caller = "")
        {
            return new SimpleQueue<string>(new MsmqMessageQueue(caller));
        }
    }
}
