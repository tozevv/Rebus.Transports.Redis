using System.Runtime.Remoting.Messaging;

namespace Rebus.Transports.Redis.Tests
{
    using MsgPack.Serialization;
    using Rebus.Bus;
    using System;
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
            if (Transaction.Current == null)
            {
                return new NoTransaction();
            }

            // store ambient transaction in logical data context
            AmbientTransactionContext trans = CallContext.LogicalGetData("ambientTrans")
                as AmbientTransactionContext;

            if (trans == null)
            {
                trans = new AmbientTransactionContext();
                CallContext.LogicalSetData("ambientTrans", trans);
                Transaction.Current.TransactionCompleted += (o, e) => {
                    CallContext.LogicalSetData("ambientTrans", null);
                };
            }
            return trans;
        }

        public void Send(T message, TimeSpan? expire = null)
        {
            var transactionContext = GetCurrentTransactionContext();

            var transportMessage = new TransportMessageToSend
            {
                Body = serializer.PackSingleObject(message),
                Label = typeof(T).FullName
            };

            if (expire.HasValue)
            {
                transportMessage.Headers = new Dictionary<string, object>();
                transportMessage.Headers.Add(Rebus.Shared.Headers.TimeToBeReceived, expire.ToString());
            }
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
