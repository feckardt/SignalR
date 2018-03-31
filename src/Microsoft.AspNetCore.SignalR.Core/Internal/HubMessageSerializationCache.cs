using System;
using System.Buffers;
using System.Collections.Generic;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;

namespace Microsoft.AspNetCore.SignalR.Internal
{
    public class HubMessageSerializationCache
    {
        public HubMessage Message { get; }

        public HubMessageSerializationCache(HubMessage message)
        {
            Message = message;
        }

        public ReadOnlyMemory<byte> GetSerializedMessage(IHubProtocol protocol)
        {
            // Pseudo code:
            // if(Cache.TryGetValue(protocol.Name, out var buffer)) {
            //   return buffer;
            // } else if(Message == null) {
            //   throw new Exception("Another server serialized this message but didn't have the protocol used by this connection registered!");
            // } else {
            //   var buffer = Serialize(Message, protocol);
            //   Cache[protocol] = buffer;
            //   return buffer;
            // }
            throw new NotImplementedException();
        }

        public void WriteAllSerializedVersions(IBufferWriter<byte> writer, IReadOnlyList<IHubProtocol> protocols)
        {
            // Write all the serialized versions!
            throw new NotImplementedException();
        }

        public static HubMessageSerializationCache ReadAllSerializedVersions(ref ReadOnlySequence<byte> buffer)
        {
            // This will return a cache with a null Message. But that's OK because GetSerializedMessage will return
            // values without having to serialize.
            throw new NotImplementedException();
        }
    }
}
