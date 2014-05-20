namespace Rebus.Redis.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Controls redis server start, stop and failure.
    /// </summary>
    public static class RedisServer
    {
        private static string redisPath = @"..\packages\Redis-64.2.8.4";
        private static Process redisProcess; 

        public static void Start() 
        {
            redisProcess = Process.Start(Path.Combine(redisPath, "redis-server.exe restart"));
        }

        public static void Stop() 
        {
            Process.Start(Path.Combine(redisPath, "redis-server.exe stop"));
        }

        public static void Crash()
        {
            redisProcess.Kill();
        }
    }
}
