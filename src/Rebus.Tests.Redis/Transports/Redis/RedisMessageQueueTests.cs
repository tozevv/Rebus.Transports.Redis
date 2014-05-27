namespace Rebus.Tests.Transports.Redis
{
    using System.Text;

    using NUnit.Framework;
    using Rebus.Transports.Redis;
    using Rebus.Bus;
    using StackExchange.Redis;
    using System.Transactions;

    /// <summary>
    /// Unit tests for Redis Message Queue.
    /// </summary>
    [TestFixture]
    public class RedisMessageQueueTests
    {
        private readonly string queueName = "sampleQueue";

        private RedisServer server = null;

        [TestFixtureSetUp]
        public void Init()
        {
            server = new RedisServer(6666);
            server.Start();
        }

        [TestFixtureTearDown]
        public void Dispose()
        {
            server.Stop();
        }

        [Test]
        public void WhenSendingMessage_ThenMessageIsDelivered()
        {
            var transactionContext = new NoTransaction();

            string sentMessage = "message";
            var configuration = server.ClientConfiguration;

            var queue = new RedisMessageQueue(configuration, this.queueName);

            queue.Send(this.queueName, CreateStringMessage(sentMessage), transactionContext);
            var receivedMessage = GetStringMessage(queue.ReceiveMessage(transactionContext));

            Assert.AreEqual(sentMessage, receivedMessage);
        }

        [Test]
        public void WhenTransactionCommitted_MessageIsSent()
        {
            var transactionScope = new TransactionScope();
            //automatically enslists on transaction scope
            var transactionContext = new Rebus.Bus.AmbientTransactionContext();

            string sentMessage = "message";
            var configuration = server.ClientConfiguration;

            var queue = new RedisMessageQueue(configuration, this.queueName);

            queue.Send(this.queueName, CreateStringMessage(sentMessage), transactionContext);

            var receivedMessage = GetStringMessage(queue.ReceiveMessage(transactionContext));
            Assert.IsNull(receivedMessage);

            //prepare transaction for commit
            transactionScope.Complete();

            receivedMessage = GetStringMessage(queue.ReceiveMessage(transactionContext));
            //still null because transaction is not yet committed
            Assert.IsNull(receivedMessage);

            //do commit transaction
            transactionScope.Dispose();

            receivedMessage = GetStringMessage(queue.ReceiveMessage(transactionContext));
            Assert.AreEqual(sentMessage, receivedMessage);
        }

        protected TransportMessageToSend CreateStringMessage(string contents)
        {
            return
                new TransportMessageToSend
            {
                Body = Encoding.UTF8.GetBytes(contents),
                Label = typeof(string).FullName
            };
        }

        protected string GetStringMessage(ReceivedTransportMessage message)
        {
            return message == null ? null :
                Encoding.UTF8.GetString(message.Body);
        }


    }
}
