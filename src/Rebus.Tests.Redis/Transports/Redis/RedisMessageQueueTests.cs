namespace Rebus.Tests.Transports.Redis
{
	using System.Text;

    using NUnit.Framework;
    using Rebus.Transports.Redis;
	using Rebus.Bus;
	using StackExchange.Redis;

    /// <summary>
    /// Unit tests for Redis Message Queue.
    /// </summary>
    [TestFixture]
    public class RedisMessageQueueTests
    {
		private RedisServer server = null;
        [TestFixtureSetUp]
        public void Init()
        {
			server = new RedisServer (6666);
			server.Start ();
        }

        [TestFixtureTearDown]
        public void Dispose()
        {
			server.Stop ();
        }

        [Test]
		public void WhenSendingMessage_ThenMessageIsDelivered()
        {
			var transactionContext = new NoTransaction ();
			string queueName = "sampleQueue";
			string sentMessage = "message";
			var configuration = server.ClientConfiguration;

			var queue = new RedisMessageQueue(configuration, queueName);

			queue.Send (queueName, CreateStringMessage(sentMessage), transactionContext);
			var receivedMessage = GetStringMessage(queue.ReceiveMessage (transactionContext));

			Assert.AreEqual (sentMessage, receivedMessage);
        }

		private TransportMessageToSend CreateStringMessage(string contents)
		{
			return
				new TransportMessageToSend
			{
				Body = Encoding.UTF8.GetBytes(contents),
				Label = typeof(string).FullName
			};
		}

		private string GetStringMessage(ReceivedTransportMessage message)
		{
			return message == null ? null :
				Encoding.UTF8.GetString (message.Body);
		}


    }
}
