namespace Rebus.Tests.Transports.Redis
{
    using MsgPack.Serialization;

    /// <summary>
    /// Helpers to create messages and convert them to and from transport messages
    /// </summary>
    public class MessagePacker<T> where T:class
    {
        private readonly MessagePackSerializer<T> serializer;

        public MessagePacker()
        {
            serializer =  MessagePackSerializer.Get<T>();
        }

        public TransportMessageToSend Pack(T contents)
        {
            return
                new TransportMessageToSend
                {
                    Body = serializer.PackSingleObject(contents),
                    Label = typeof(T).FullName
                };
        }

        public T Unpack(ReceivedTransportMessage message)
        {
            return message == null ? null :
               serializer.UnpackSingleObject(message.Body);
        }
    }
}
