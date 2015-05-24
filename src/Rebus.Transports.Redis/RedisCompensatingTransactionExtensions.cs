using System;
using StackExchange.Redis;

namespace Rebus.Transports.Redis
{
    public static class RedisCompensatingTransactionExtensions
    {
        public static RedisResult ScriptEvaluate(string script, 
            RedisKey[] keys = null, 
            RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {


        }



            private const string LogCompensationScript = @"
            local numbertobytes = function(num, width)
              local function _n2b(t, width, num, rem)
                if width == 0 then return table.concat(t) end
                table.insert(t, 1, string.char(rem * 256))
                return _n2b(t, width-1, math.modf(num/256))
              end
              return _n2b({}, width, math.modf(num/256))
            end

            local commandtostring = function(...)
                local result = ''
                for i,v in ipairs({...}) do
                    local arg = tostring(v)
                    local size = string.len(arg)
                    result = result .. numbertobytes(size, 4) .. arg
                end
                return result
            end

            local logcompensation = function(transactionKey, ...)
                local strcmd = commandtostring(...)
                redis.call('LPUSH', transactionKey, strcmd)
            end";

        private const string RunCompensationScript = @"
            local bytestonumber = function(str)
              local function _b2n(num, digit, ...)
                if not digit then return num end
                return _b2n(num*256 + digit, ...)
              end
              return _b2n(0, string.byte(str, 1, -1))
            end

            local stringtocommand = function(str)
                local result = {}
                local i = 4
                while (i < string.len(str)) do
                    local size = bytestonumber(string.sub(str, i - 3, i))
                    local arg = string.sub(str, i + 1, i + size)
                    table.insert(result, arg)
                    i = i + size + 4
                end
                return result
            end

            local runcompensation = function(transactionKey)
                while (true) do
                    local strcmd = redis.call('LPOP', transactionKey)
                    if (strcmd == false) then
                        return
                    end
                    local cmd = stringtocommand(strcmd)
                    redis.call(unpack(cmd))
                end
            end";
    }
}

