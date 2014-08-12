namespace Rebus.Transports.Redis
{
    using System;
    using StackExchange.Redis;

    public class MessageQueueKey
    {
        private const string keyFormat = "rebus:queue:{0}";

        public MessageQueueKey(string queueName)
        {
            this.QueueName = queueName;
            this.Key = string.Format(keyFormat, queueName);
        }

        public MessageQueueKey(RedisKey key)
        {
            this.QueueName = key.ToString().Substring(0, keyFormat.Length - 4);
            this.Key = key;
        }

        public RedisKey Key
        {
            get;
            private set;
        }

        public string QueueName
        {
            get;
            private set;
        }
    }
}

