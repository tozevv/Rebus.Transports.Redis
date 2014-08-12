namespace Rebus.Transports.Redis
{
    using System;
    using StackExchange.Redis;

    public class MessageQueueTransactionKey
    {
        private const string keyFormat = "rebus:queue:{0}:transaction:{1}";
  

        public MessageQueueTransactionKey(string queueName, long transactionId)
        {
            this.QueueName = queueName;
            this.TransactionId = transactionId;
            this.Key = string.Format(keyFormat, queueName, transactionId);
        }

        public MessageQueueTransactionKey(RedisKey key)
        {
            string keyString = key.ToString();
            string transactionFragment = keyString.Substring(keyString.LastIndexOf(":"));
            this.TransactionId = long.Parse(transactionFragment);
            this.QueueName = key.ToString().Substring(0, transactionFragment.Length - 13);
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

        public long TransactionId
        {
            get;
            private set;
        }

        public MessageQueueKey MessageQueue
        {
            get 
            {
                return new MessageQueueKey(this.QueueName);
            }
        }

        public static string MatchPattern
        {
            get 
            {
                return string.Format(keyFormat, "*", "*");
            }
        }
    }
}

