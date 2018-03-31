using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;

namespace Microsoft.AspNetCore.SignalR.Internal
{
    // This class is created at the same time as the wrapping HubMessage and has the exact same lifetime as that message.
    // See DefaultHubLifetimeManager.CreateInvocationMessage
    public class HubMessageSerializationCache
    {
        private SerializedMessage _cachedItem1;
        private SerializedMessage _cachedItem2;
        private IDictionary<string, byte[]> _cachedItems;

        public HubMessage Message { get; }

        public HubMessageSerializationCache(HubMessage message)
        {
            Message = message;
        }

        public ReadOnlyMemory<byte> GetSerializedMessage(IHubProtocol protocol)
        {
            if (!TryGetCached(protocol.Name, out var serialized))
            {
                if (Message == null)
                {
                    throw new InvalidOperationException(
                        "This message was received from another server that did not have the requested protocol available.");
                }

                serialized = protocol.WriteToArray(Message);
                SetCache(protocol.Name, serialized);
            }

            return serialized;
        }

        public void WriteAllSerializedVersions(IBufferWriter<byte> writer, IReadOnlyList<IHubProtocol> protocols)
        {
            // We use MsgPack, but not a MsgPack serialization library. The format is just fine for us, so we may as well
            // use something simple.

            MiniMsgPack.WriteMapHeader(writer, protocols.Count);
            foreach (var protocol in protocols)
            {
                MiniMsgPack.WriteUtf8(writer, protocol.Name);

                var buffer = GetSerializedMessage(protocol);
                MiniMsgPack.WriteBinHeader(writer, buffer.Length);
                writer.Write(buffer);
            }
        }

        public static HubMessageSerializationCache ReadAllSerializedVersions(ref ReadOnlySequence<byte> buffer)
        {
            // This will return a cache with a null Message. But that's OK because GetSerializedMessage will return
            // values without having to serialize.
            throw new NotImplementedException();
        }


        private void SetCache(string protocolName, byte[] serialized)
        {
            if (_cachedItem1.ProtocolName == null)
            {
                _cachedItem1.ProtocolName = protocolName;
                _cachedItem1.Serialized = serialized;
            }
            else if (_cachedItem2.ProtocolName == null)
            {
                _cachedItem2.ProtocolName = protocolName;
                _cachedItem2.Serialized = serialized;
            }
            else
            {
                if (_cachedItems == null)
                {
                    _cachedItems = new Dictionary<string, byte[]>();
                }

                _cachedItems[protocolName] = serialized;
            }
        }

        private bool TryGetCached(string protocolName, out byte[] result)
        {
            if (string.Equals(_cachedItem1.ProtocolName, protocolName, StringComparison.Ordinal))
            {
                result = _cachedItem1.Serialized;
                return true;
            }

            if (string.Equals(_cachedItem2.ProtocolName, protocolName, StringComparison.Ordinal))
            {
                result = _cachedItem2.Serialized;
                return true;
            }

            if (_cachedItems != null)
            {
                return _cachedItems.TryGetValue(protocolName, out result);
            }

            result = default;
            return false;
        }

        private struct SerializedMessage
        {
            public string ProtocolName { get; set; }
            public byte[] Serialized { get; set; }
        }
    }
}
