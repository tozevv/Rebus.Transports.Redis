using System;
using System.Linq;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace Rebus.Transports.Redis
{
    public class CompensatingTransaction
    {
        private readonly IDatabase database;
        private readonly ITransaction commitTransaction = null;
        private readonly string transactionLog = null;

        internal CompensatingTransaction(IDatabase database, ITransaction commitTransaction, string transactionLog)
        {
            this.database = database;
            this.commitTransaction = commitTransaction;
            this.transactionLog = transactionLog;

        }

        /// <summary>
        /// Evaluate async on commit and therefore does not require compensation.
        /// </summary>
        /// <returns>The script evaluation result.</returns>
        /// <param name="script">Script to execute.</param>
        /// <param name="keys">Keys script parameters.</param>
        /// <param name="values">Values script parameters.</param>
        /// <param name="flags">Script flags.</param>
        public Task<RedisResult> ScriptEvaluateAsync(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            return this.commitTransaction.ScriptEvaluateAsync(script, keys, values, flags);
        }

        /// <summary>
        /// Scripts  evaluate immediately and provide compensation semantigs
        /// </summary>
        /// <returns>The script evaluation result.</returns>
        /// <param name="script">Script to execute.</param>
        /// <param name="keys">Keys script parameters.</param>
        /// <param name="values">Values script parameters.</param>
        /// <param name="flags">Script flags.</param>
        public RedisResult ScriptEvaluate(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            const string CompensateScript = @"
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

                local compensate = function(...)
                    local strcmd = commandtostring(...)
                    redis.call('LPUSH', KEYS[KEYPOS], strcmd)
                end";

            if (keys == null)
            {
                keys = new RedisKey[] {};
            }

            // Use replace instead of string.Format to prevent escaping Luas {} curly braces 
            string combinedScript = CompensateScript.Replace("KEYPOS", (keys.Length + 1).ToString())
                                    + script;
     
            RedisKey[] combinedKeys = keys.Concat(new RedisKey[] { this.transactionLog }).ToArray();
           
            return this.database.ScriptEvaluate(combinedScript, combinedKeys, values, flags);
        }

        public void Commit()
        {
            this.commitTransaction.KeyDeleteAsync(this.transactionLog);
            this.commitTransaction.Execute(CommandFlags.PreferMaster);
        }

        public void Rollback()
        {
            Rollback(this.transactionLog);    
        }

        private void Rollback(string transactionKey)
        { 
            this.database.ScriptEvaluate(@"
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

                local rollback = function(key) 
                    while (true) do
                        local strcmd = redis.call('RPOP', key)
                       
                        if (strcmd == false) then
                            return
                        end

                        local cmd = stringtocommand(strcmd)
                       
                        redis.call(unpack(cmd))

                    end
                end

                rollback(KEYS[1])
                ", new RedisKey[] { this.transactionLog });
        }
    }
}

