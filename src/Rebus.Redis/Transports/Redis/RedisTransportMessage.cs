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
    }
}