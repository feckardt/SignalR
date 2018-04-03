// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;
using Microsoft.AspNetCore.SignalR.Redis.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace Microsoft.AspNetCore.SignalR.Redis
{
    public class RedisHubLifetimeManager<THub> : HubLifetimeManager<THub>, IDisposable where THub : Hub
    {
        private readonly HubConnectionStore _connections = new HubConnectionStore();
        // TODO: Investigate "memory leak" entries never get removed
        private readonly ConcurrentDictionary<string, GroupData> _groups = new ConcurrentDictionary<string, GroupData>();
        private readonly IConnectionMultiplexer _redisServerConnection;
        private readonly ISubscriber _bus;
        private readonly ILogger _logger;
        private readonly RedisOptions _options;
        private readonly RedisChannels _channels;
        private readonly string _serverName = GenerateServerName();
        private readonly IReadOnlyList<IHubProtocol> _hubProtocols;

        private readonly AckHandler _ackHandler;
        private int _internalId;

        // This serializer is ONLY use to transmit the data through redis, it has no connection to the serializer used on each connection.
        private readonly JsonSerializer _serializer = new JsonSerializer
        {
            TypeNameHandling = TypeNameHandling.None,
            Formatting = Formatting.None,
        };

        public RedisHubLifetimeManager(ILogger<RedisHubLifetimeManager<THub>> logger,
                                       IOptions<RedisOptions> options,
                                       IHubProtocolResolver hubProtocolResolver)
        {
            _logger = logger;
            _options = options.Value;
            _ackHandler = new AckHandler();
            _channels = new RedisChannels(typeof(THub).FullName);
            _hubProtocols = hubProtocolResolver.AllProtocols;

            var writer = new LoggerTextWriter(logger);
            RedisLog.ConnectingToEndpoints(_logger, options.Value.Options.EndPoints, _serverName);
            _redisServerConnection = _options.Connect(writer);

            _redisServerConnection.ConnectionRestored += (_, e) =>
            {
                // We use the subscription connection type
                // Ignore messages from the interactive connection (avoids duplicates)
                if (e.ConnectionType == ConnectionType.Interactive)
                {
                    return;
                }

                RedisLog.ConnectionRestored(_logger);
            };

            _redisServerConnection.ConnectionFailed += (_, e) =>
            {
                // We use the subscription connection type
                // Ignore messages from the interactive connection (avoids duplicates)
                if (e.ConnectionType == ConnectionType.Interactive)
                {
                    return;
                }

                RedisLog.ConnectionFailed(_logger, e.Exception);
            };

            if (_redisServerConnection.IsConnected)
            {
                RedisLog.Connected(_logger);
            }
            else
            {
                RedisLog.NotConnected(_logger);
            }
            _bus = _redisServerConnection.GetSubscriber();

            SubscribeToAll();
            SubscribeToGroupManagementChannel();
            SubscribeToAckChannel();
        }

        public override Task OnConnectedAsync(HubConnectionContext connection)
        {
            var feature = new RedisFeature();
            connection.Features.Set<IRedisFeature>(feature);

            var redisSubscriptions = feature.Subscriptions;
            var connectionTask = Task.CompletedTask;
            var userTask = Task.CompletedTask;

            _connections.Add(connection);

            connectionTask = SubscribeToConnection(connection, redisSubscriptions);

            if (!string.IsNullOrEmpty(connection.UserIdentifier))
            {
                userTask = SubscribeToUser(connection, redisSubscriptions);
            }

            return Task.WhenAll(connectionTask, userTask);
        }

        public override Task OnDisconnectedAsync(HubConnectionContext connection)
        {
            _connections.Remove(connection);

            var tasks = new List<Task>();

            var feature = connection.Features.Get<IRedisFeature>();

            var redisSubscriptions = feature.Subscriptions;
            if (redisSubscriptions != null)
            {
                foreach (var subscription in redisSubscriptions)
                {
                    RedisLog.Unsubscribe(_logger, subscription);
                    tasks.Add(_bus.UnsubscribeAsync(subscription));
                }
            }

            var groupNames = feature.Groups;

            if (groupNames != null)
            {
                // Copy the groups to an array here because they get removed from this collection
                // in RemoveGroupAsync
                foreach (var group in groupNames.ToArray())
                {
                    // Use RemoveGroupAsyncCore because the connection is local and we don't want to
                    // accidentally go to other servers with our remove request.
                    tasks.Add(RemoveGroupAsyncCore(connection, group));
                }
            }

            return Task.WhenAll(tasks);
        }

        public override Task SendAllAsync(string methodName, object[] args)
        {
            var message = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args), _hubProtocols);
            return PublishAsync(_channels.All, message);
        }

        public override Task SendAllExceptAsync(string methodName, object[] args, IReadOnlyList<string> excludedIds)
        {
            var message = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args, excludedIds), _hubProtocols);
            return PublishAsync(_channels.All, message);
        }

        public override Task SendConnectionAsync(string connectionId, string methodName, object[] args)
        {
            if (connectionId == null)
            {
                throw new ArgumentNullException(nameof(connectionId));
            }

            // If the connection is local we can skip sending the message through the bus since we require sticky connections.
            // This also saves serializing and deserializing the message!
            var connection = _connections[connectionId];
            if (connection != null)
            {
                return SafeWriteAsync(connection, new InvocationMessage(methodName, argumentBindingException: null, args));
            }

            var message = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args), _hubProtocols);
            return PublishAsync(_channels.Connection(connectionId), message);
        }

        public override Task SendGroupAsync(string groupName, string methodName, object[] args)
        {
            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

            var message = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args), _hubProtocols);
            return PublishAsync(_channels.Group(groupName), message);
        }

        public override Task SendGroupExceptAsync(string groupName, string methodName, object[] args, IReadOnlyList<string> excludedIds)
        {
            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

            var message = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args, excludedIds), _hubProtocols);
            return PublishAsync(_channels.Group(groupName), message);
        }

        public override Task SendUserAsync(string userId, string methodName, object[] args)
        {
            var message = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args), _hubProtocols);
            return PublishAsync(_channels.User(userId), message);
        }

        public override async Task AddGroupAsync(string connectionId, string groupName)
        {
            if (connectionId == null)
            {
                throw new ArgumentNullException(nameof(connectionId));
            }

            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }

            var connection = _connections[connectionId];
            if (connection != null)
            {
                // short circuit if connection is on this server
                await AddGroupAsyncCore(connection, groupName);
                return;
            }

            await SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Add);
        }

        public override async Task RemoveGroupAsync(string connectionId, string groupName)
        {
            if (connectionId == null)
            {
                throw new ArgumentNullException(nameof(connectionId));
            }

            if (groupName == null)
            {
                throw new ArgumentNullException(nameof(groupName));
            }


            var connection = _connections[connectionId];
            if (connection != null)
            {
                // short circuit if connection is on this server
                await RemoveGroupAsyncCore(connection, groupName);
                return;
            }

            await SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Remove);
        }

        public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object[] args)
        {
            if (connectionIds == null)
            {
                throw new ArgumentNullException(nameof(connectionIds));
            }
            var publishTasks = new List<Task>(connectionIds.Count);
            var invocation = RedisInvocation.Create(methodName, args);

            byte[] payload = null;

            foreach (string connectionId in connectionIds)
            {
                var connection = _connections[connectionId];
                // If the connection is local we can skip sending the message through the bus since we require sticky connections.
                // This also saves serializing and deserializing the message!
                if (connection != null)
                {
                    publishTasks.Add(SafeWriteAsync(connection, invocation.Message));
                }
                else
                {
                    if (payload == null)
                    {
                        payload = RedisProtocol.WriteInvocation(invocation, _hubProtocols);
                    }

                    publishTasks.Add(PublishAsync(_channels.Connection(connectionId), payload));
                }
            }

            return Task.WhenAll(publishTasks);
        }

        public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object[] args)
        {
            if (groupNames == null)
            {
                throw new ArgumentNullException(nameof(groupNames));
            }
            var publishTasks = new List<Task>(groupNames.Count);
            var payload = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args), _hubProtocols);

            foreach (var groupName in groupNames)
            {
                if (!string.IsNullOrEmpty(groupName))
                {
                    publishTasks.Add(PublishAsync(_channels.Group(groupName), payload));
                }
            }

            return Task.WhenAll(publishTasks);
        }

        public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object[] args)
        {
            if (userIds.Count > 0)
            {
                var payload = RedisProtocol.WriteInvocation(RedisInvocation.Create(methodName, args), _hubProtocols);
                var publishTasks = new List<Task>(userIds.Count);
                foreach (var userId in userIds)
                {
                    if (!string.IsNullOrEmpty(userId))
                    {
                        publishTasks.Add(PublishAsync(_channels.User(userId), payload));
                    }
                }

                return Task.WhenAll(publishTasks);
            }

            return Task.CompletedTask;
        }

        private async Task PublishAsync(string channel, byte[] payload)
        {
            RedisLog.PublishToChannel(_logger, channel);
            await _bus.PublishAsync(channel, payload);
        }

        private async Task AddGroupAsyncCore(HubConnectionContext connection, string groupName)
        {
            var feature = connection.Features.Get<IRedisFeature>();
            var groupNames = feature.Groups;

            lock (groupNames)
            {
                // Connection already in group
                if (!groupNames.Add(groupName))
                {
                    return;
                }
            }

            var groupChannel = _channels.Group(groupName);
            var group = _groups.GetOrAdd(groupChannel, _ => new GroupData());

            await group.Lock.WaitAsync();
            try
            {
                group.Connections.Add(connection);

                // Subscribe once
                if (group.Connections.Count > 1)
                {
                    return;
                }

                await SubscribeToGroup(groupChannel, group);
            }
            finally
            {
                group.Lock.Release();
            }
        }

        /// <summary>
        /// This takes <see cref="HubConnectionContext"/> because we want to remove the connection from the
        /// _connections list in OnDisconnectedAsync and still be able to remove groups with this method.
        /// </summary>
        private async Task RemoveGroupAsyncCore(HubConnectionContext connection, string groupName)
        {
            var groupChannel = _channels.Group(groupName);

            if (!_groups.TryGetValue(groupChannel, out var group))
            {
                return;
            }

            var feature = connection.Features.Get<IRedisFeature>();
            var groupNames = feature.Groups;
            if (groupNames != null)
            {
                lock (groupNames)
                {
                    groupNames.Remove(groupName);
                }
            }

            await group.Lock.WaitAsync();
            try
            {
                if (group.Connections.Count > 0)
                {
                    group.Connections.Remove(connection);

                    if (group.Connections.Count == 0)
                    {
                        RedisLog.Unsubscribe(_logger, groupChannel);
                        await _bus.UnsubscribeAsync(groupChannel);
                    }
                }
            }
            finally
            {
                group.Lock.Release();
            }
        }

        private async Task SendGroupActionAndWaitForAck(string connectionId, string groupName, GroupAction action)
        {
            var id = Interlocked.Increment(ref _internalId);
            var ack = _ackHandler.CreateAck(id);
            // Send Add/Remove Group to other servers and wait for an ack or timeout
            var message = RedisProtocol.WriteGroupCommand(new RedisGroupCommand(id, _serverName, action, groupName, connectionId));
            await PublishAsync(_channels.GroupManagement, message);

            await ack;
        }

        public void Dispose()
        {
            _bus.UnsubscribeAll();
            _redisServerConnection.Dispose();
            _ackHandler.Dispose();
        }

        private T DeserializeMessage<T>(RedisValue data)
        {
            using (var reader = new JsonTextReader(new StreamReader(new MemoryStream(data))))
            {
                return _serializer.Deserialize<T>(reader);
            }
        }

        private void SubscribeToAll()
        {
            RedisLog.Subscribing(_logger, _channels.All);
            _bus.Subscribe(_channels.All, async (c, data) =>
            {
                try
                {
                    RedisLog.ReceivedFromChannel(_logger, _channels.All);

                    var invocation = RedisProtocol.ReadInvocation(data);

                    var tasks = new List<Task>(_connections.Count);

                    foreach (var connection in _connections)
                    {
                        if (invocation.ExcludedIds == null || invocation.ExcludedIds.Contains(connection.ConnectionId))
                        {
                            tasks.Add(SafeWriteAsync(connection, invocation.Message));
                        }
                    }

                    await Task.WhenAll(tasks);
                }
                catch (Exception ex)
                {
                    RedisLog.FailedWritingMessage(_logger, ex);
                }
            });
        }

        private void SubscribeToGroupManagementChannel()
        {
            _bus.Subscribe(_channels.GroupManagement, async (c, data) =>
            {
                try
                {
                    var groupMessage = RedisProtocol.ReadGroupCommand(data);

                    var connection = _connections[groupMessage.ConnectionId];
                    if (connection == null)
                    {
                        // user not on this server
                        return;
                    }

                    if (groupMessage.Action == GroupAction.Remove)
                    {
                        await RemoveGroupAsyncCore(connection, groupMessage.GroupName);
                    }

                    if (groupMessage.Action == GroupAction.Add)
                    {
                        await AddGroupAsyncCore(connection, groupMessage.GroupName);
                    }

                    // Send an ack to the server that sent the original command.
                    await PublishAsync(_channels.Ack(groupMessage.ServerName), RedisProtocol.WriteAck(groupMessage.Id));
                }
                catch (Exception ex)
                {
                    RedisLog.InternalMessageFailed(_logger, ex);
                }
            });
        }

        private void SubscribeToAckChannel()
        {
            // Create server specific channel in order to send an ack to a single server
            _bus.Subscribe(_channels.Ack(_serverName), (c, data) =>
            {
                var ackId = RedisProtocol.ReadAck(data);

                _ackHandler.TriggerAck(ackId);
            });
        }

        private Task SubscribeToConnection(HubConnectionContext connection, HashSet<string> redisSubscriptions)
        {
            var connectionChannel = _channels.Connection(connection.ConnectionId);
            redisSubscriptions.Add(connectionChannel);

            RedisLog.Subscribing(_logger, connectionChannel);
            return _bus.SubscribeAsync(connectionChannel, async (c, data) =>
            {
                var invocation = RedisProtocol.ReadInvocation(data);
                await SafeWriteAsync(connection, invocation.Message);
            });
        }

        private Task SubscribeToUser(HubConnectionContext connection, HashSet<string> redisSubscriptions)
        {
            var userChannel = _channels.User(connection.UserIdentifier);
            redisSubscriptions.Add(userChannel);

            // TODO: Look at optimizing (looping over connections checking for Name)
            return _bus.SubscribeAsync(userChannel, async (c, data) =>
            {
                var invocation = RedisProtocol.ReadInvocation(data);
                await SafeWriteAsync(connection, invocation.Message);
            });
        }

        private Task SubscribeToGroup(string groupChannel, GroupData group)
        {
            RedisLog.Subscribing(_logger, groupChannel);
            return _bus.SubscribeAsync(groupChannel, async (c, data) =>
            {
                try
                {
                    var invocation = RedisProtocol.ReadInvocation(data);

                    var tasks = new List<Task>();
                    foreach (var groupConnection in group.Connections)
                    {
                        if (invocation.ExcludedIds?.Contains(groupConnection.ConnectionId) == true)
                        {
                            continue;
                        }

                        tasks.Add(SafeWriteAsync(groupConnection, invocation.Message));
                    }

                    await Task.WhenAll(tasks);
                }
                catch (Exception ex)
                {
                    RedisLog.FailedWritingMessage(_logger, ex);
                }
            });
        }

        // This methods are to protect against connections throwing synchronously when writing to them and preventing other connections from being written to
        private async Task SafeWriteAsync(HubConnectionContext connection, HubMessageSerializationCache message)
        {
            try
            {
                await connection.WriteAsync(message);
            }
            catch (Exception ex)
            {
                RedisLog.FailedWritingMessage(_logger, ex);
            }
        }

        private async Task SafeWriteAsync(HubConnectionContext connection, HubMessage message)
        {
            try
            {
                await connection.WriteAsync(message);
            }
            catch (Exception ex)
            {
                RedisLog.FailedWritingMessage(_logger, ex);
            }
        }

        private static string GenerateServerName()
        {
            // Use the machine name for convenience diagnostics, but add a guid to make it unique.
            // Example: MyServerName_02db60e5fab243b890a847fa5c4dcb29
            return $"{Environment.MachineName}_{Guid.NewGuid():N}";
        }

        private class LoggerTextWriter : TextWriter
        {
            private readonly ILogger _logger;

            public LoggerTextWriter(ILogger logger)
            {
                _logger = logger;
            }

            public override Encoding Encoding => Encoding.UTF8;

            public override void Write(char value)
            {

            }

            public override void WriteLine(string value)
            {
                RedisLog.ConnectionMultiplexerMessage(_logger, value);
            }
        }

        private class GroupData
        {
            public SemaphoreSlim Lock = new SemaphoreSlim(1, 1);
            public HubConnectionStore Connections = new HubConnectionStore();
        }

        private interface IRedisFeature
        {
            HashSet<string> Subscriptions { get; }
            HashSet<string> Groups { get; }
        }

        private class RedisFeature : IRedisFeature
        {
            public HashSet<string> Subscriptions { get; } = new HashSet<string>();
            public HashSet<string> Groups { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
