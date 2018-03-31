using System;
using System.Buffers;
using System.Buffers.Binary;

namespace Microsoft.AspNetCore.SignalR.Internal
{
    internal static class MiniMsgPack
    {
        public static unsafe void WriteMapHeader(IBufferWriter<byte> writer, int count)
        {
            Span<byte> buf = stackalloc byte[5];
            if (count < 16)
            {
                buf[0] = (byte)((count & 0x0F) | 0x80);
                writer.Write(buf.Slice(0, 1));
            }
            else if (count < ushort.MaxValue)
            {
                buf[0] = 0xDE;
                BinaryPrimitives.TryWriteUInt16BigEndian(buf.Slice(1, 2), (ushort)count);
                writer.Write(buf.Slice(0, 3));
            }
            else
            {
                buf[0] = 0xDF;
                BinaryPrimitives.TryWriteUInt32BigEndian(buf.Slice(1, 4), (ushort)count);
                writer.Write(buf);
            }
        }

        public static unsafe void WriteStringHeader(IBufferWriter<byte> writer, int count)
        {
            Span<byte> buf = stackalloc byte[5];
            if (count < 32)
            {
                buf[0] = (byte)((count & 0x1F) | 0xA0);
                writer.Write(buf.Slice(0, 1));
            }
            else if (count < byte.MaxValue)
            {
                buf[0] = 0xD9;
                buf[1] = (byte)count;
                writer.Write(buf.Slice(0, 2));
            }
            else if (count < ushort.MaxValue)
            {
                buf[0] = 0xDA;
                BinaryPrimitives.TryWriteUInt16BigEndian(buf.Slice(1, 2), (ushort)count);
                writer.Write(buf.Slice(0, 3));
            }
            else
            {
                buf[0] = 0xDB;
                BinaryPrimitives.TryWriteUInt32BigEndian(buf.Slice(1, 4), (ushort)count);
                writer.Write(buf);
            }
        }

        public static unsafe void WriteBinHeader(IBufferWriter<byte> writer, int count)
        {
            Span<byte> buf = stackalloc byte[5];
            if (count < byte.MaxValue)
            {
                buf[0] = 0xC4;
                buf[1] = (byte)count;
                writer.Write(buf.Slice(0, 2));
            }
            else if (count < ushort.MaxValue)
            {
                buf[0] = 0xC5;
                BinaryPrimitives.TryWriteUInt16BigEndian(buf.Slice(1, 2), (ushort)count);
                writer.Write(buf.Slice(0, 3));
            }
            else
            {
                buf[0] = 0xC6;
                BinaryPrimitives.TryWriteUInt32BigEndian(buf.Slice(1, 4), (ushort)count);
                writer.Write(buf);
            }
        }
    }
}
