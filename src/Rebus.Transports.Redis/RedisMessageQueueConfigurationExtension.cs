namespace Rebus.Transports.Redis
{
	using System;
	using Rebus.Bus;
	using Rebus.Configuration;
	using StackExchange.Redis;

	/// <summary>
	/// Configuration helper for the Redis Message Queue.
	/// </summary>
	public static class RedisMessageQueueConfigurationExtension
	{
		private const string ConfigurationErrorMessage = @"
            An error occurred when trying to parse out the configuration of the RebusConfigurationSection:

            {0}

            -

            For this way of configuring input queue to work, you need to supply a correct configuration
            section declaration in the <configSections> element of your app.config/web.config - like so:

            <configSections>
            <section name=""rebus"" type=""Rebus.Configuration.RebusConfigurationSection, Rebus"" />
            <!-- other stuff in here as well -->
            </configSections>

            -and then you need a <rebus> element some place further down the app.config/web.config,
            like so:

            <rebus inputQueue=""my.service.input.queue"" errorQueue=""my.service.error.queue"" />

            Note also, that specifying the input queue name with the 'inputQueue' attribute is optional.

            A more full example configuration snippet can be seen here:

            {1}";

        /// <summary>
        /// Uses the redis message queue transport.
        /// </summary>
        /// <param name="configurer">Configurer.</param>
        /// <param name="redisConnectionString">Redis connection string.</param>
        /// <param name="inputQueue">Input queue name.</param>
        /// <param name="errorQueue">Error queue name.</param>
        /// <param name="transactionTimeout">Optional transaction timeout for redis. Default is 30s.</param>
        public static void UseRedis(this RebusTransportConfigurer configurer, string redisConnectionString, string inputQueue, string errorQueue, 
            TimeSpan? transactionTimeout = null)
		{
            UseRedis(configurer, ConfigurationOptions.Parse(redisConnectionString), inputQueue, errorQueue, transactionTimeout);
		}

        /// <summary>
        /// Uses the redis message queue transport.
        /// </summary>
        /// <param name="configurer">Configurer.</param>
        /// <param name="options">Redis connection options.</param>
        /// <param name="inputQueue">Input queue name.</param>
        /// <param name="errorQueue">Error queue name.</param>
        /// <param name="transactionTimeout">Optional transaction timeout for redis. Default is 30s.</param>
        public static void UseRedis(this RebusTransportConfigurer configurer, ConfigurationOptions redisConnection, string inputQueue, string errorQueue,
            TimeSpan? transactionTimeout = null)
		{
			if (string.IsNullOrEmpty(inputQueue))
			{
				throw new ConfigurationException("You need to specify an input queue.");
			}
            transactionTimeout = transactionTimeout ?? TimeSpan.FromSeconds(30);
            var redisMessageQueue = new RedisMessageQueue(redisConnection, inputQueue, transactionTimeout);

			configurer.UseSender(redisMessageQueue);
			configurer.UseReceiver(redisMessageQueue);
			configurer.UseErrorTracker(new ErrorTracker(errorQueue));
		}

        /// <summary>
        /// Uses the redis and get input queue name from app config.
        /// </summary>
        /// <param name="configurer">Configurer.</param>
        /// <param name="redisConnectionString">Redis connection string.</param>
        /// <param name="transactionTimeout">Optional transaction timeout for redis. Default is 30s.</param>
        public static void UseRedisAndGetInputQueueNameFromAppConfig(this RebusTransportConfigurer configurer, 
            string redisConnectionString, TimeSpan? transactionTimeout = null)
		{
			try
			{
				var section = RebusConfigurationSection.LookItUp();
				section.VerifyPresenceOfInputQueueConfig();
				section.VerifyPresenceOfErrorQueueConfig();

				var inputQueueName = section.InputQueue;
				var errorQueueName = section.ErrorQueue;

                UseRedis(configurer, redisConnectionString, inputQueueName, errorQueueName, transactionTimeout);
			}
			catch (RedisConnectionException)
			{
				throw;
			}
			catch (Exception ex)
			{
				throw new ConfigurationException(ConfigurationErrorMessage, ex, RebusConfigurationSection.ExampleSnippetForErrorMessages);
			}
		}
	}
}