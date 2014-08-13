using System;

namespace Rebus.Redis
{
    internal static class Keys
    {
        private const string keyFormat = "rebus:queue:{0}";

        RedisKey ForQueue(string queue, string 
    }
}

