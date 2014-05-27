namespace Rebus.Transports.Redis
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;

    /// <summary>
    /// Message record stored in Redis.
    /// </summary>
    [Serializable]
    internal class RedisTransportMessage
    {
        public byte[] Body { get; set; }

        public IDictionary<string, object> Headers { get; set; }

        public string Id { get; set; }

        public string Label { get; set; }

        public static RedisTransportMessage Deserialize(string value)
        {
            byte[] b = Convert.FromBase64String(value);
            using (var stream = new MemoryStream(b))
            {
                var formatter = new BinaryFormatter();
                stream.Seek(0, SeekOrigin.Begin);
                return (RedisTransportMessage)formatter.Deserialize(stream);
            }
        }

        public string Serialize()
        {
            using (var stream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(stream, this);
                stream.Flush();
                stream.Position = 0;
                return Convert.ToBase64String(stream.ToArray());
            }
        }
    }
}