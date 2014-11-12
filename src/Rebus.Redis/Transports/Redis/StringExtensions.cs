namespace Rebus.Transports.Redis
{
    using System.Linq;
    using System.Text.RegularExpressions;

    public static class StringExtensions
    {
        public static string ParseFormat(this string input, string format, int position)
        {
            object[] groupings = format.Where(f => f == '{').Select(f => (object)"(.+)").ToArray();
       
            Regex regex = new Regex("^" + string.Format(format, groupings) + "$");
            return regex.Matches(input)[0].Groups[1 + position].Value;
        }
    }
}
    