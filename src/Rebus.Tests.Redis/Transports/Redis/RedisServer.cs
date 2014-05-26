namespace Rebus.Tests.Transports.Redis
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
	using StackExchange.Redis;

    /// <summary>
    /// Controls redis server start, stop and failure.
    /// </summary>
    public class RedisServer
    {
		private const string redisWindowsPath = @"Redis-64.2.8.4";
		private const string redisMacPath = @"Redis-64.2.8.9.macosx";
        
		private Process process; 
		private readonly string serverCommand = null;
		private readonly string clientCommand = null;

		private readonly int port;

		public RedisServer(int port)
		{
			this.port = port;
			// assume unix is mac for now...
			if (Environment.OSVersion.Platform == PlatformID.Unix || 
				Environment.OSVersion.Platform == PlatformID.MacOSX) 
			{
				serverCommand = Path.Combine("..", "..", "..", "packages", redisMacPath, "redis-server");
				clientCommand = Path.Combine("..", "..", "..", "packages", redisMacPath, "redis-cli");
			} else 
			{
				serverCommand = Path.Combine("..", "..", "..", "packages", redisWindowsPath, "redis-server.exe");
				clientCommand = Path.Combine("..", "..", "..", "packages", redisWindowsPath, "redis-cli.exe");
			}
			this.ClientConfiguration = new ConfigurationOptions() 
			{
				EndPoints =
				{
					{ "127.0.0.1", this.port },
				},
				KeepAlive = 180
			};
		}

        public void Start() 
        {
			Stop (); // Confirm is not running...
	
			process = Process.Start(new ProcessStartInfo() {
				FileName = serverCommand,
				UseShellExecute = true,
				Arguments = string.Format("--port {0}", port)
			});

			process.WaitForExit (500);

			if (process.HasExited) 
			{
				throw new Exception ("Cannot start redis server");
			}
        }

        public void Stop() 
        {
			Process.Start (clientCommand, string.Format ("-p {0} shutdown", port)).WaitForExit ();
        }

        public void Crash()
        {
            process.Kill();
        }

		public ConfigurationOptions ClientConfiguration 
		{
			get;
			private set;
		}
    }
}
