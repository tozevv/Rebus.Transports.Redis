namespace Rebus.Transports.Redis
{
	using System;
	using System.Collections.Generic;

    /// <summary>
	/// Message record stored in Redis.
	/// </summary>
	[Serializable]
	public class RedisTransportMessage

	{
        public RedisTransportMessage()
        {
        }

        public RedisTransportMessage(TransportMessageToSend send)
        {
            Body = send.Body;
            Headers = send.Headers;
            Label = send.Label;
        }

		public byte[] Body { get; set; }

		public IDictionary<string, object> Headers { get; set; }

		public string Label { get; set; }
        
        public ReceivedTransportMessage ToReceivedTransportMessage(string id)
        {
            return new ReceivedTransportMessage()
            {
                Id = id,
                Body = this.Body,
                Headers = this.Headers,
                Label = this.Label
            };
        }

        /// <summary>
        /// Retrieve the message expiration from the message headers
        /// </summary>
        /// <returns>Message expiration if available</returns>
        public TimeSpan? GetMessageExpiration()
        {
            object timeoutString = string.Empty;
            if (Headers.TryGetValue(Rebus.Shared.Headers.TimeToBeReceived, out timeoutString) &&
                (timeoutString is string))
            {
                return TimeSpan.Parse(timeoutString as string);
            }
            else
            {
                return null;
            }
        }
	}
}