namespace Rebus.Tests.Transports.Redis
{
    using System;
    using Rebus.Transports.Redis;
    using NUnit.Framework;

    [TestFixture]
    public class StringExtensionsTests
    {
        [Test]
        [TestCase("param0:param1:param2", "{0}:{1}:{2}", 0, Result = "param0")]
        [TestCase("param0:param1:param2", "{0}:{1}:{2}", 1, Result = "param1")]
        [TestCase("param0:param1:param2", "{0}:{1}:{2}", 2, Result = "param2")]
        [TestCase("otherstuff:param0:param1:param2", "{0}:{1}:{2}", 0, Result = "otherstuff:param0")]
        [TestCase("otherstuff:param0:param1:param2", "{0}:{1}:{2}", 1, Result = "param1")]
        [TestCase("otherstuff:param0:param1:param2", "{0}:{1}:{2}", 2, Result = "param2")]
        public string TestParseString(string input, string format, int position)
        {
            return input.ParseFormat(format, position);
        }
    }
}

