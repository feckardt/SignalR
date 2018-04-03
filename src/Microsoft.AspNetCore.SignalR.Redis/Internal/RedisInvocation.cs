using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;

namespace Microsoft.AspNetCore.SignalR.Redis.Internal
{
    public struct RedisInvocation
    {
        /// <summary>
        /// Gets a list of connections that should be excluded from this invocation.
        /// May be null to indicate that no connections are to be excluded.
        /// </summary>
        public IReadOnlyList<string> ExcludedIds { get; }

        /// <summary>
        /// Gets the message serialization cache containing serialized payloads for the message.
        /// </summary>
        public HubMessageSerializationCache Message { get; }

        public RedisInvocation(HubMessageSerializationCache message, IReadOnlyList<string> excludedIds)
        {
            Message = message;
            ExcludedIds = excludedIds;
        }

        public static RedisInvocation Create(string target, object[] arguments, IReadOnlyList<string> excludedIds = null)
        {
            return new RedisInvocation(
                new HubMessageSerializationCache(new InvocationMessage(target, argumentBindingException: null, arguments)),
                excludedIds);
        }
    }
}
