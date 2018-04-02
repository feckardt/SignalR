using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

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

        private static readonly int StackPathMaxCharacterCount = 32;
        private const int MaxBytesPerUtf8Character = 4;
        public static void WriteUtf8(IBufferWriter<byte> writer, string value)
        {
            if (value.Length > StackPathMaxCharacterCount)
            {
                // Slow path, we will never get here in our usage of this type because
                // our protocol names are short.
                var bytes = Encoding.UTF8.GetBytes(value);
                WriteStringHeader(writer, bytes.Length);
                writer.Write(bytes);
            }
            else
            {
                unsafe
                {
                    // Faster path, we know we'll need less than 128 bytes to encode, so we can
                    // use a stack buffer to marshal the data (and get the length)
                    Span<byte> bytes = stackalloc byte[value.Length * MaxBytesPerUtf8Character];
#if NETCOREAPP2_1
                    var size = Encoding.UTF8.GetBytes(value.AsSpan(), bytes);
                    bytes = bytes.Slice(0, size);
#else
                    fixed (byte* bytePtr = &MemoryMarshal.GetReference(bytes))
                    fixed (char* charPtr = value)
                    {
                        var size = Encoding.UTF8.GetBytes(charPtr, value.Length, bytePtr, bytes.Length);
                        bytes = bytes.Slice(0, size);
                    }
#endif
                    writer.Write(bytes);
                }

            }
        }
    }
}
