// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;
using Microsoft.AspNetCore.SignalR.Tests;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using MsgPack.Serialization;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Xunit;

namespace Microsoft.AspNetCore.SignalR.Redis.Tests
{
    public class RedisHubLifetimeManagerTests
    {
        [Fact]
        public async Task InvokeAllAsyncWritesToAllConnectionsOutput()
        {
            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                var manager = CreateLifetimeManager();
                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager.OnConnectedAsync(connection1).OrTimeout();
                await manager.OnConnectedAsync(connection2).OrTimeout();

                await manager.SendAllAsync("Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client1);
                await AssertMessageAsync(client2);
            }
        }

        [Fact]
        public async Task InvokeAllExceptAsyncExcludesSpecifiedConnections()
        {
            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            using (var client3 = new TestClient())
            {
                var manager1 = CreateLifetimeManager();
                var manager2 = CreateLifetimeManager();
                var manager3 = CreateLifetimeManager();

                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);
                var connection3 = HubConnectionContextUtils.Create(client3.Connection);

                await manager1.OnConnectedAsync(connection1).OrTimeout();
                await manager2.OnConnectedAsync(connection2).OrTimeout();
                await manager3.OnConnectedAsync(connection3).OrTimeout();

                await manager1.SendAllExceptAsync("Hello", new object[] { "World" }, new [] { client3.Connection.ConnectionId }).OrTimeout();

                await AssertMessageAsync(client1);
                await AssertMessageAsync(client2);
                Assert.Null(client3.TryRead());
            }
        }

        [Fact]
        public async Task InvokeAllAsyncDoesNotWriteToDisconnectedConnectionsOutput()
        {
            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                var manager = CreateLifetimeManager();
                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager.OnConnectedAsync(connection1).OrTimeout();
                await manager.OnConnectedAsync(connection2).OrTimeout();

                await manager.OnDisconnectedAsync(connection2).OrTimeout();

                await manager.SendAllAsync("Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client1);

                Assert.Null(client2.TryRead());
            }
        }

        [Fact]
        public async Task InvokeGroupAsyncWritesToAllConnectionsInGroupOutput()
        {
            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                var manager = CreateLifetimeManager();
                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager.OnConnectedAsync(connection1).OrTimeout();
                await manager.OnConnectedAsync(connection2).OrTimeout();

                await manager.AddGroupAsync(connection1.ConnectionId, "gunit").OrTimeout();

                await manager.SendGroupAsync("gunit", "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client1);
                Assert.Null(client2.TryRead());
            }
        }

        [Fact]
        public async Task InvokeGroupExceptAsyncWritesToAllValidConnectionsInGroupOutput()
        {
            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                var manager = CreateLifetimeManager();
                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager.OnConnectedAsync(connection1).OrTimeout();
                await manager.OnConnectedAsync(connection2).OrTimeout();

                await manager.AddGroupAsync(connection1.ConnectionId, "gunit").OrTimeout();
                await manager.AddGroupAsync(connection2.ConnectionId, "gunit").OrTimeout();

                var excludedIds = new List<string> { client2.Connection.ConnectionId };
                await manager.SendGroupExceptAsync("gunit", "Hello", new object[] { "World" }, excludedIds).OrTimeout();

                await AssertMessageAsync(client1);
                Assert.Null(client2.TryRead());
            }
        }

        [Fact]
        public async Task InvokeConnectionAsyncWritesToConnectionOutput()
        {
            using (var client = new TestClient())
            {
                var manager = CreateLifetimeManager();
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager.OnConnectedAsync(connection).OrTimeout();

                await manager.SendConnectionAsync(connection.ConnectionId, "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
            }
        }

        [Fact]
        public async Task InvokeConnectionAsyncOnNonExistentConnectionDoesNotThrow()
        {
            var manager = CreateLifetimeManager();
            await manager.SendConnectionAsync("NotARealConnectionId", "Hello", new object[] { "World" }).OrTimeout();
        }

        [Fact]
        public async Task InvokeAllAsyncWithMultipleServersWritesToAllConnectionsOutput()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager1.OnConnectedAsync(connection1).OrTimeout();
                await manager2.OnConnectedAsync(connection2).OrTimeout();

                await manager1.SendAllAsync("Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client1);
                await AssertMessageAsync(client2);
            }
        }

        [Fact]
        public async Task InvokeAllAsyncWithMultipleServersDoesNotWriteToDisconnectedConnectionsOutput()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager1.OnConnectedAsync(connection1).OrTimeout();
                await manager2.OnConnectedAsync(connection2).OrTimeout();

                await manager2.OnDisconnectedAsync(connection2).OrTimeout();

                await manager2.SendAllAsync("Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client1);

                Assert.Null(client2.TryRead());
            }
        }

        [Fact]
        public async Task InvokeConnectionAsyncOnServerWithoutConnectionWritesOutputToConnection()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager1.OnConnectedAsync(connection).OrTimeout();

                await manager2.SendConnectionAsync(connection.ConnectionId, "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
            }
        }

        [Fact]
        public async Task InvokeGroupAsyncOnServerWithoutConnectionWritesOutputToGroupConnection()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager1.OnConnectedAsync(connection).OrTimeout();

                await manager1.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager2.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
            }
        }

        [Fact]
        public async Task DisconnectConnectionRemovesConnectionFromGroup()
        {
            var manager = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager.OnConnectedAsync(connection).OrTimeout();

                await manager.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager.OnDisconnectedAsync(connection).OrTimeout();

                await manager.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                Assert.Null(client.TryRead());
            }
        }

        [Fact]
        public async Task RemoveGroupFromLocalConnectionNotInGroupDoesNothing()
        {
            var manager = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager.OnConnectedAsync(connection).OrTimeout();

                await manager.RemoveGroupAsync(connection.ConnectionId, "name").OrTimeout();
            }
        }

        [Fact]
        public async Task RemoveGroupFromConnectionOnDifferentServerNotInGroupDoesNothing()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager1.OnConnectedAsync(connection).OrTimeout();

                await manager2.RemoveGroupAsync(connection.ConnectionId, "name").OrTimeout();
            }
        }

        [Fact]
        public async Task AddGroupAsyncForConnectionOnDifferentServerWorks()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager1.OnConnectedAsync(connection).OrTimeout();

                await manager2.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager2.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
            }
        }

        [Fact]
        public async Task AddGroupAsyncForLocalConnectionAlreadyInGroupDoesNothing()
        {
            var manager = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager.OnConnectedAsync(connection).OrTimeout();

                await manager.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();
                await manager.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
                Assert.Null(client.TryRead());
            }
        }

        [Fact]
        public async Task AddGroupAsyncForConnectionOnDifferentServerAlreadyInGroupDoesNothing()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager1.OnConnectedAsync(connection).OrTimeout();

                await manager1.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();
                await manager2.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager2.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
                Assert.Null(client.TryRead());
            }
        }

        [Fact]
        public async Task RemoveGroupAsyncForConnectionOnDifferentServerWorks()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                await manager1.OnConnectedAsync(connection).OrTimeout();

                await manager1.AddGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager2.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);

                await manager2.RemoveGroupAsync(connection.ConnectionId, "name").OrTimeout();

                await manager2.SendGroupAsync("name", "Hello", new object[] { "World" }).OrTimeout();

                Assert.Null(client.TryRead());
            }
        }

        [Fact]
        public async Task InvokeConnectionAsyncForLocalConnectionDoesNotPublishToRedis()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                var connection = HubConnectionContextUtils.Create(client.Connection);

                // Add connection to both "servers" to see if connection receives message twice
                await manager1.OnConnectedAsync(connection).OrTimeout();
                await manager2.OnConnectedAsync(connection).OrTimeout();

                await manager1.SendConnectionAsync(connection.ConnectionId, "Hello", new object[] { "World" }).OrTimeout();

                await AssertMessageAsync(client);
                Assert.Null(client.TryRead());
            }
        }

        [Fact]
        public async Task WritingToRemoteConnectionThatFailsDoesNotThrow()
        {
            var manager1 = CreateLifetimeManager();
            var manager2 = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                // Force an exception when writing to connection
                var connectionMock = HubConnectionContextUtils.CreateMock(client.Connection);
                connectionMock.Setup(m => m.WriteAsync(It.IsAny<HubMessage>())).Throws(new Exception());
                var connection = connectionMock.Object;

                await manager2.OnConnectedAsync(connection).OrTimeout();

                // This doesn't throw because there is no connection.ConnectionId on this server so it has to publish to redis.
                // And once that happens there is no way to know if the invocation was successful or not.
                await manager1.SendConnectionAsync(connection.ConnectionId, "Hello", new object[] { "World" }).OrTimeout();
            }
        }

        [Fact]
        public async Task WritingToLocalConnectionThatFailsDoesNotThrowException()
        {
            var manager = CreateLifetimeManager();

            using (var client = new TestClient())
            {
                // Force an exception when writing to connection
                var connectionMock = HubConnectionContextUtils.CreateMock(client.Connection);
                connectionMock.Setup(m => m.WriteAsync(It.IsAny<HubMessage>())).Throws(new Exception("Message"));
                var connection = connectionMock.Object;

                await manager.OnConnectedAsync(connection).OrTimeout();

                await manager.SendConnectionAsync(connection.ConnectionId, "Hello", new object[] { "World" }).OrTimeout();
            }
        }

        [Fact]
        public async Task WritingToGroupWithOneConnectionFailingSecondConnectionStillReceivesMessage()
        {
            var manager = CreateLifetimeManager();

            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                // Force an exception when writing to connection
                var connectionMock = HubConnectionContextUtils.CreateMock(client1.Connection);
                connectionMock.Setup(m => m.WriteAsync(It.IsAny<HubMessage>())).Throws(new Exception());

                var connection1 = connectionMock.Object;
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager.OnConnectedAsync(connection1).OrTimeout();
                await manager.AddGroupAsync(connection1.ConnectionId, "group");
                await manager.OnConnectedAsync(connection2).OrTimeout();
                await manager.AddGroupAsync(connection2.ConnectionId, "group");

                await manager.SendGroupAsync("group", "Hello", new object[] { "World" }).OrTimeout();
                // connection1 will throw when receiving a group message, we are making sure other connections
                // are not affected by another connection throwing
                await AssertMessageAsync(client2);

                // Repeat to check that group can still be sent to
                await manager.SendGroupAsync("group", "Hello", new object[] { "World" }).OrTimeout();
                await AssertMessageAsync(client2);
            }
        }

        [Fact]
        public async Task CamelCasedJsonIsPreservedAcrossRedisBoundary()
        {
            var messagePackOptions = new MessagePackHubProtocolOptions();
            messagePackOptions.SerializationContext.DictionarySerlaizationOptions.KeyTransformer = DictionaryKeyTransformers.LowerCamel;

            var jsonOptions = new JsonHubProtocolOptions();
            jsonOptions.PayloadSerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();

            using (var client1 = new TestClient())
            using (var client2 = new TestClient())
            {
                // The sending manager has serializer settings
                var manager1 = CreateLifetimeManager(
                    messagePackOptions: messagePackOptions,
                    jsonOptions: jsonOptions);

                // The receiving one doesn't matter because of how we serialize!
                var manager2 = CreateLifetimeManager();

                var connection1 = HubConnectionContextUtils.Create(client1.Connection);
                var connection2 = HubConnectionContextUtils.Create(client2.Connection);

                await manager1.OnConnectedAsync(connection1).OrTimeout();
                await manager2.OnConnectedAsync(connection2).OrTimeout();

                await manager1.SendAllAsync("Hello", new object[] { new TestObject { TestProperty = "Foo" } });

                var message = Assert.IsType<InvocationMessage>(await client2.ReadAsync().OrTimeout());
                Assert.Equal("Hello", message.Target);
                Assert.Collection(
                    message.Arguments,
                    arg0 =>
                    {
                        var dict = Assert.IsType<JObject>(arg0);
                        Assert.Collection(dict.Properties(),
                            prop =>
                            {
                                Assert.Equal("testProperty", prop.Name);
                                Assert.Equal("Foo", prop.Value.Value<string>());
                            });
                    });
            }
        }

        public class TestObject
        {
            public string TestProperty { get; set; }
        }

        private RedisHubLifetimeManager<MyHub> CreateLifetimeManager(MessagePackHubProtocolOptions messagePackOptions = null, JsonHubProtocolOptions jsonOptions = null)
        {
            var options = new RedisOptions() { Factory = t => new TestConnectionMultiplexer() };
            messagePackOptions = messagePackOptions ?? new MessagePackHubProtocolOptions();
            jsonOptions = jsonOptions ?? new JsonHubProtocolOptions();

            return new RedisHubLifetimeManager<MyHub>(
                NullLogger<RedisHubLifetimeManager<MyHub>>.Instance,
                Options.Create(options),
                new DefaultHubProtocolResolver(new IHubProtocol[]
                {
                    new JsonHubProtocol(Options.Create(jsonOptions)),
                    new MessagePackHubProtocol(Options.Create(messagePackOptions)),
                }, NullLogger<DefaultHubProtocolResolver>.Instance));
        }

        private async Task AssertMessageAsync(TestClient client)
        {
            var message = Assert.IsType<InvocationMessage>(await client.ReadAsync().OrTimeout());
            Assert.Equal("Hello", message.Target);
            Assert.Single(message.Arguments);
            Assert.Equal("World", (string)message.Arguments[0]);
        }

        private class MyHub : Hub
        {
        }

        private class MockChannel : Channel<HubMessage>
        {
            public MockChannel(ChannelWriter<HubMessage> writer = null)
            {
                Writer = writer;
            }
        }
    }
}
