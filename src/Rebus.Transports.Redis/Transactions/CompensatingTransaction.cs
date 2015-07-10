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
        private readonly string transactionKey = null;

        private const string createTx = @"
            local createtx = function() 
                local transactionId = redis.call('INCR', 'transaction:counter')
                local transactionKey = 'transaction:' .. transactionId
                
                redis.call('ZADD', 'transaction:current', transactionId, transactionKey)
                
                local transactionTimeout = redis.call('GET', 'transaction:timeout')
                if  (transactionTimeout == false) then
                    transactionTimeout = 60
                end

                local transactionLock = transactionKey .. ':lock'
                redis.call('SETEX', transactionLock, transactionTimeout, '1')

                return transactionKey
            end
        ";
        
        private const string disposeTx = @"
            local disposetx = function(key) 
                redis.call('DEL', key)
                redis.call('DEL', key .. ':lock')
                redis.call('ZREM', 'transaction:current', key)
            end
        ";

        private const string rollbackScript = disposeTx +
           @"
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
                            break
                        end

                        local cmd = stringtocommand(strcmd)
                       
                        redis.call(unpack(cmd))
                    end

                    disposetx(key)
                end";
        
        internal CompensatingTransaction(IDatabase database, ITransaction commitTransaction, string transactionKey)
        {
            this.database = database;
            this.commitTransaction = commitTransaction;
            this.transactionKey = transactionKey;
        }

        public static CompensatingTransaction BeginTransaction(IDatabase database) 
        {
            string transactionKey = 
                (string)database.ScriptEvaluate(@createTx + " return createtx()");
            
            ITransaction commitTransaction = database.CreateTransaction();

            commitTransaction.AddCondition(Condition.KeyExists(transactionKey + ":lock"));
            commitTransaction.ScriptEvaluateAsync(@disposeTx + " return disposetx()");

            return new CompensatingTransaction(database, commitTransaction, transactionKey);
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
     
            RedisKey[] combinedKeys = keys.Concat(new RedisKey[] { this.transactionKey }).ToArray();
           
            return this.database.ScriptEvaluate(combinedScript, combinedKeys, values, flags);
        }

        public void Commit()
        {
            //EnsureTimeoutsRollback();
            if (!this.commitTransaction.Execute(CommandFlags.PreferMaster))
            {
                throw new TimeoutException("Transaction timed out");
            }
        }

        public void Rollback()
        {
            this.database.ScriptEvaluate(rollbackScript + " return rollback(KEYS[1])", 
                new RedisKey[] { this.transactionKey });
        }

        public void EnsureTimeoutsRollback() 
        {
            RedisResult nextExpiring = this.database.ScriptEvaluate(rollbackScript + @"
                while (true) 
                    local transactionKey = redis.call('transaction:current', 0, 0);
                    if (transactionKey == false) then 
                        local transactionTimeout = redis.call('GET', 'transaction:timeout')
                        if  (transactionTimeout == false) then
                            transactionTimeout = 60
                        end
                        return transactionTimeout
                    end

                    local ttl = redis.call('TTL', current .. ':lock')

                    if (ttl < 0) then
                        rollback(transactionKey) 
                    else
                        return ttl
                    end 
                end
            ", null, null, CommandFlags.None);
        }
    }
}

