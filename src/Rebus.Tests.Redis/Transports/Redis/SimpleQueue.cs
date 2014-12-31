namespace Rebus.Tests.Transports.Redis
{
    using MsgPack.Serialization;
    using Rebus.Bus;
    using System.Collections.Generic;
    using System.Transactions;

    public class SimpleQueue<T>  where T:class
    {
        private readonly MessagePackSerializer<T> serializer;
        private readonly IDuplexTransport transport;
  

        public SimpleQueue(IDuplexTransport transport)
        {
            this.serializer =  MessagePackSerializer.Get<T>();
            this.transport = transport;
        }

        public ITransactionContext GetCurrentTransactionContext()
        {
            return Transaction.Current == null ? new NoTransaction() as ITransactionContext : new Rebus.Bus.AmbientTransactionContext();
        }

        public void Send(T message)
        {
            var transactionContext = GetCurrentTransactionContext();

            var transportMessage = new TransportMessageToSend
            {
                Body = serializer.PackSingleObject(message),
                Label = typeof(T).FullName
            };
            transport.Send(this.transport.InputQueue, transportMessage, transactionContext);
        }

        public T Receive()
        {
            var transactionContext = GetCurrentTransactionContext();
         
            ReceivedTransportMessage message = transport.ReceiveMessage(transactionContext);
            return message == null ? null :
               serializer.UnpackSingleObject(message.Body);
        }

        public void SendAll(IEnumerable<T> messages)
        {
            foreach (var message in messages)
            {
                Send(message);
            }
        }

        public IEnumerable<T> ReceiveAll()
        {
            T receivedMessage;
            List<T> receivedMessages = new List<T>();
            while ((receivedMessage =Receive()) != null)
            {
                receivedMessages.Add(receivedMessage);
            }
            return receivedMessages;
        }
    }
}
