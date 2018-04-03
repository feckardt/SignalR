// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;

namespace Microsoft.AspNetCore.SignalR.Redis.Internal
{
    public static class RedisProtocol
    {
        private static readonly Encoding _utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

        // REVIEW: This protocol is simplistic and probably inefficient as is. Discuss.

        // The Redis Protocol:
        // * The message type is known in advance because messages are sent to different channels based on type
        // * Invocations are sent to the All, Group, Connection and User channels
        // * Group Commands are sent to the GroupManagement channel
        // * Acks are sent to the Acknowledgement channel.
        // * See the Write[type] methods for a description of the protocol for each in-depth.
        // * The "Variable length integer" is the length-prefixing format used by BinaryReader/BinaryWriter:
        //   * https://docs.microsoft.com/en-us/dotnet/api/system.io.binarywriter.write?view=netstandard-2.0
        // * The "Length prefixed string" is the string format used by BinaryReader/BinaryWriter:
        //   * A 7-bit variable length integer encodes the length in bytes, followed by the encoded string in UTF-8.

        public static byte[] WriteInvocation(RedisInvocation invocation, IReadOnlyList<IHubProtocol> protocols)
        {
            // Redis Invocation Format:
            // * Variable length integer: Number of excluded Ids
            // * For each excluded Id:
            //   * Length prefixed string: ID
            // * HubMessageSerializationCache encoded by the format described by that type.

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriterWithVarInt(stream, _utf8NoBom))
            {
                if (invocation.ExcludedIds != null)
                {
                    writer.WriteVarInt(invocation.ExcludedIds.Count);
                    foreach (var id in invocation.ExcludedIds)
                    {
                        writer.Write(id);
                    }
                }
                else
                {
                    writer.WriteVarInt(0);
                }

                invocation.Message.WriteAllSerializedVersions(writer, protocols);
                return stream.ToArray();
            }
        }

        public static byte[] WriteGroupCommand(RedisGroupCommand command)
        {
            // Group Command Format:
            // * Variable length integer: Id
            // * Length prefixed string: ServerName
            // * 1 byte: Action
            // * Length prefixed string: GroupName
            // * Length prefixed string: ConnectionId

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriterWithVarInt(stream, _utf8NoBom))
            {
                writer.WriteVarInt(command.Id);
                writer.Write(command.ServerName);
                writer.Write((byte)command.Action);
                writer.Write(command.GroupName);
                writer.Write(command.ConnectionId);
                return stream.ToArray();
            }
        }

        public static byte[] WriteAck(int messageId)
        {
            // Acknowledgement Format:
            // * Variable length integer: Id

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriterWithVarInt(stream, _utf8NoBom))
            {
                writer.WriteVarInt(messageId);
                return stream.ToArray();
            }
        }

        public static RedisInvocation ReadInvocation(byte[] data)
        {
            // See WriteInvocation for format.

            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReaderWithVarInt(stream, _utf8NoBom))
            {
                IReadOnlyList<string> excludedIds = null;

                var idCount = reader.ReadVarInt();
                if (idCount > 0)
                {
                    var ids = new string[idCount];
                    for (var i = 0; i < idCount; i++)
                    {
                        ids[i] = reader.ReadString();
                    }

                    excludedIds = ids;
                }

                var message = HubMessageSerializationCache.ReadAllSerializedVersions(reader);
                return new RedisInvocation(message, excludedIds);
            }
        }

        public static RedisGroupCommand ReadGroupCommand(byte[] data)
        {
            // See WriteGroupCommand for format.
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReaderWithVarInt(stream, _utf8NoBom))
            {
                var id = reader.ReadVarInt();
                var serverName = reader.ReadString();
                var action = (GroupAction) reader.ReadByte();
                var groupName = reader.ReadString();
                var connectionId = reader.ReadString();

                return new RedisGroupCommand(id, serverName, action, groupName, connectionId);
            }
        }

        public static int ReadAck(byte[] data)
        {
            // See WriteAck for format
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReaderWithVarInt(stream, _utf8NoBom))
            {
                return reader.ReadVarInt();
            }
        }

        // Kinda cheaty way to get access to write the 7-bit varint format directly
        private class BinaryWriterWithVarInt : BinaryWriter
        {
            public BinaryWriterWithVarInt(Stream output, Encoding encoding) : base(output, encoding)
            {
            }

            public void WriteVarInt(int value) => Write7BitEncodedInt(value);
        }

        private class BinaryReaderWithVarInt : BinaryReader
        {
            public BinaryReaderWithVarInt(Stream input, Encoding encoding) : base(input, encoding)
            {
            }

            public int ReadVarInt() => Read7BitEncodedInt();
        }
    }
}