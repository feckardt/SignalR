// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.SignalR.Internal;
using StackExchange.Redis;

namespace Microsoft.AspNetCore.SignalR.Redis.Internal
{
    public static class RedisProtocol
    {
        public static byte[] WriteInvocation(RedisInvocation invocation)
        {
        }

        public static byte[] WriteGroupCommand(RedisGroupCommand command)
        {
        }

        public static byte[] WriteAck(int groupMessageId)
        {
        }

        public static RedisInvocation ReadInvocation(byte[] data)
        {
        }

        public static RedisGroupCommand ReadGroupCommand(byte[] data)
        {
        }

        public static int ReadAck(byte[] data)
        {
        }
    }
}
