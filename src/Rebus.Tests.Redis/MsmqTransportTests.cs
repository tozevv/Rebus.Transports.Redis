namespace Rebus.Tests.Transports.Redis
{
    using NUnit.Framework;
    using Rebus.Transports.Msmq;
    using System;
    using System.Messaging;

    [TestFixture(typeof(MsmqMessageQueue))]
    public class MsmqTransportTests : TransportTestsBase<MsmqMessageQueue>
    {
        private readonly bool msmqInstalled = false;
        private static object lockObject = new object();

        public MsmqTransportTests(Type t)
            : base()
        {
            this.msmqInstalled = CheckMSMQAvailable();
        }

        protected override IDuplexTransport GetTransport(string queueName)
        {
            if (this.msmqInstalled)
            {
                return new MsmqMessageQueue(queueName) as IDuplexTransport;
            }
            else
            {
                Assert.Inconclusive("MSMQ not available.");
                return null;
            }
        }

        protected static bool CheckMSMQAvailable()
        {
            // make sure we run onle one of this tests at any time.
            lock (lockObject)
            {
                try
                {
                    string queueName = @".\Private$\RebusRedisUnitTestQueue";

                    if (!MessageQueue.Exists(queueName))
                    {
                        MessageQueue.Create(queueName);
                    }

                    MessageQueue messageQueue = new MessageQueue(queueName);
                    messageQueue.Label = "RebusRedisUnitTestQueue";
                    string sent = "test";

                    messageQueue.Send(sent);
                    return messageQueue.Receive().Body as string == sent;
                }
                catch
                {
                    return false;
                }
            }
        }
    }
}
