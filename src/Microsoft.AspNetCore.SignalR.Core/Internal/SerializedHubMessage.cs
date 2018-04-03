using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;

namespace Microsoft.AspNetCore.SignalR.Internal
{
    /// <summary>
    /// This class is designed to support the framework. The API is subject to breaking changes.
    /// Represents a serialization cache for a single message.
    /// </summary>
    public class SerializedHubMessage
    {
        private SerializedMessage _cachedItem1;
        private SerializedMessage _cachedItem2;
        private IDictionary<string, byte[]> _cachedItems;

        public HubMessage Message { get; }

        private SerializedHubMessage()
        {
        }

        public SerializedHubMessage(HubMessage message)
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

        public static void WriteAllSerializedVersions(BinaryWriter writer, HubMessage message, IReadOnlyList<IHubProtocol> protocols)
        {
            // The serialization format is based on BinaryWriter
            // * 1 byte number of protocols
            // * For each protocol:
            //   * Length-prefixed string using 7-bit variable length encoding (length depends on BinaryWriter's encoding)
            //   * 4 byte length of the buffer
            //   * N byte buffer

            if (protocols.Count > byte.MaxValue)
            {
                throw new InvalidOperationException($"Can't serialize cache containing more than {byte.MaxValue} entries");
            }

            writer.Write((byte)protocols.Count);
            foreach (var protocol in protocols)
            {
                writer.Write(protocol.Name);

                var buffer = protocol.WriteToArray(message);
                writer.Write(buffer.Length);
                writer.Write(buffer);
            }
        }

        public static SerializedHubMessage ReadAllSerializedVersions(BinaryReader reader)
        {
            var cache = new SerializedHubMessage();
            var count = reader.ReadByte();
            for (var i = 0; i < count; i++)
            {
                var protocol = reader.ReadString();
                var length = reader.ReadInt32();
                var serialized = reader.ReadBytes(length);
                cache.SetCache(protocol, serialized);
            }

            return cache;
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
