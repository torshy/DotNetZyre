using System;
using System.Net;
using System.Net.Sockets;
using NetMQ;
using NetMQ.Sockets;

namespace DotNetZyre
{
    public class ZreBeacon : IDisposable, ISocketPollable
    {
        #region Fields

        public const string ConfigureCommand = "CONFIGURE";
        public const string PublishCommand = "PUBLISH";
        public const string SilenceCommand = "SILENCE";

        /// <summary>
        /// Command to subscribe a socket to messages that have the given topic.
        /// This is valid only for Subscriber and XSubscriber sockets.
        /// </summary>
        public const string SubscribeCommand = "SUBSCRIBE";
        public const int UdpFrameMax = 255;

        /// <summary>
        /// Command to un-subscribe a socket from messages that have the given topic. 
        /// This is valid only for Subscriber and XSubscriber sockets.
        /// </summary>
        public const string UnsubscribeCommand = "UNSUBSCRIBE";

        private readonly NetMQActor _actor;
        private readonly EventDelegator<ZreBeaconEventArgs> _receiveEvent;

        #endregion Fields

        #region Constructors

        /// <summary>
        /// Create a new ZreBeacon, contained within the given context.
        /// </summary>
        /// <param name="context">the NetMQContext to contain this new socket</param>
        public ZreBeacon(NetMQContext context)
        {
            _actor = NetMQActor.Create(context, new Shim());

            EventHandler<NetMQActorEventArgs> onReceive = (sender, e) =>
                                                          _receiveEvent.Fire(this, new ZreBeaconEventArgs(this));

            _receiveEvent = new EventDelegator<ZreBeaconEventArgs>(
                () => _actor.ReceiveReady += onReceive,
                () => _actor.ReceiveReady -= onReceive);
        }

        #endregion Constructors

        #region Events

        /// <summary>
        /// This event occurs when at least one message may be received from the socket without blocking.
        /// </summary>
        public event EventHandler<ZreBeaconEventArgs> ReceiveReady
        {
            add { _receiveEvent.Event += value; }
            remove { _receiveEvent.Event -= value; }
        }

        #endregion Events

        #region Properties

        /// <summary>
        /// Ip address the beacon is bind to
        /// </summary>
        public string Hostname
        {
            get; private set;
        }

        /// <summary>
        /// Get the socket of the contained actor.
        /// </summary>
        NetMQSocket ISocketPollable.Socket
        {
            get { return ((ISocketPollable)_actor).Socket; }
        }

        #endregion Properties

        #region Methods

        /// <summary>
        /// Configure beacon to bind to all interfaces
        /// </summary>
        /// <param name="port">Port to bind to</param>
        public void ConfigureAllInterfaces(int port)
        {
            Configure("*", port);
        }

        /// <summary>
        /// Configure beacon to bind to default interface
        /// </summary>
        /// <param name="port">Port to bind to</param>
        public void Configure(int port)
        {
            Configure(string.Empty, port);
        }

        /// <summary>
        /// Configure beacon to bind to specific interface
        /// </summary>
        /// <param name="interfaceName">One of the ip address of the interface</param>
        /// <param name="port">Port to bind to</param>
        public void Configure(string interfaceName, int port)
        {
            var message = new NetMQMessage();
            message.Append(ConfigureCommand);
            message.Append(interfaceName);
            message.Append(port);

            _actor.SendMultipartMessage(message);

            Hostname = _actor.ReceiveFrameString();
        }

        /// <summary>
        /// Publish beacon immediately and continue to publish when interval elapsed
        /// </summary>
        /// <param name="transmit">Beacon to transmit</param>
        /// <param name="interval">Interval to transmit beacon</param>
        public void Publish(string transmit, TimeSpan interval)
        {
            var message = new NetMQMessage();
            message.Append(PublishCommand);
            message.Append(transmit);
            message.Append((int)interval.TotalMilliseconds);

            _actor.SendMultipartMessage(message);
        }

        /// <summary>
        /// Publish beacon immediately and continue to publish when interval elapsed
        /// </summary>
        /// <param name="transmit">Beacon to transmit</param>
        /// <param name="interval">Interval to transmit beacon</param>
        public void Publish(byte[] transmit, TimeSpan interval)
        {
            var message = new NetMQMessage();
            message.Append(PublishCommand);
            message.Append(transmit);
            message.Append((int)interval.TotalMilliseconds);

            _actor.SendMultipartMessage(message);
        }

        /// <summary>
        /// Publish beacon immediately and continue to publish every second
        /// </summary>
        /// <param name="transmit">Beacon to transmit</param>
        public void Publish(string transmit)
        {
            Publish(transmit, TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// Publish beacon immediately and continue to publish every second
        /// </summary>
        /// <param name="transmit">Beacon to transmit</param>
        public void Publish(byte[] transmit)
        {
            Publish(transmit, TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// Stop publish messages
        /// </summary>
        public void Silence()
        {
            _actor.SendFrame(SilenceCommand);
        }

        /// <summary>
        /// Subscribe to beacon messages, will replace last subscribe call
        /// </summary>
        /// <param name="filter">Beacon will be filtered by this</param>
        public void Subscribe(string filter)
        {
            _actor.SendMoreFrame(SubscribeCommand).SendFrame(filter);
        }

        /// <summary>
        /// Unsubscribe to beacon messages
        /// </summary>
        public void Unsubscribe()
        {
            _actor.SendFrame(UnsubscribeCommand);
        }

        /// <summary>
        /// Blocks until a string is received. As the returning of this method is uncontrollable, it's
        /// normally safer to call <see cref="TryReceiveString"/> instead and pass a timeout.
        /// </summary>
        /// <param name="peerName">the name of the peer, which should come before the actual message, is written to this string</param>
        /// <returns>the string that was received</returns>
        public string ReceiveString(out string peerName)
        {
            peerName = _actor.ReceiveFrameString();

            return _actor.ReceiveFrameString();
        }

        /// <summary>
        /// Attempt to receive a message from the specified peer for the specified amount of time.
        /// </summary>
        /// <param name="timeout">The maximum amount of time the call should wait for a message before returning.</param>
        /// <param name="peerName">the name of the peer that the message comes from is written to this string</param>
        /// <param name="message">the string to write the received message into</param>
        /// <returns><c>true</c> if a message was received before <paramref name="timeout"/> elapsed,
        /// otherwise <c>false</c>.</returns>
        public bool TryReceiveString(TimeSpan timeout, out string peerName, out string message)
        {
            if (!_actor.TryReceiveFrameString(timeout, out peerName))
            {
                message = null;
                return false;
            }

            return _actor.TryReceiveFrameString(timeout, out message);
        }

        /// <summary>
        /// Blocks until a message is received. As the returning of this method is uncontrollable, it's
        /// normally safer to call <see cref="TryReceiveString"/> instead and pass a timeout.
        /// </summary>
        /// <param name="peerName">the name of the peer, which should come before the actual message, is written to this string</param>
        /// <returns>the byte-array of data that was received</returns>
        public byte[] Receive(out string peerName)
        {
            peerName = _actor.ReceiveFrameString();

            return _actor.ReceiveFrameBytes();
        }

        /// <summary>
        /// Release any contained resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Release any contained resources.
        /// </summary>
        /// <param name="disposing">true if managed resources are to be released</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            _actor.Dispose();
            _receiveEvent.Dispose();
        }

        #endregion Methods

        #region Nested Types

        private class Shim : IShimHandler
        {
            #region Fields

            private EndPoint _broadcastAddress;
            private NetMQFrame _filter;
            private NetMQTimer _pingTimer;
            private NetMQSocket _pipe;
            private Poller _poller;
            private NetMQFrame _transmit;
            private int _udpPort;
            private Socket _udpSocket;

            #endregion Fields

            #region Methods

            public void Run(PairSocket shim)
            {
                _pipe = shim;

                shim.SignalOK();

                _pipe.ReceiveReady += OnPipeReady;

                _pingTimer = new NetMQTimer(0);
                _pingTimer.Elapsed += PingElapsed;
                _pingTimer.Enable = false;

                _poller = new Poller();
                _poller.AddSocket(_pipe);
                _poller.AddTimer(_pingTimer);

                _poller.PollTillCancelled();

                // the beacon might never been configured
                if (_udpSocket != null)
                {
                    _udpSocket.Close();
                }
            }

            private static bool Compare(NetMQFrame a, NetMQFrame b, int size)
            {
                for (int i = 0; i < size; i++)
                {
                    if (a.Buffer[i] != b.Buffer[i])
                    {
                        return false;
                    }
                }

                return true;
            }

            private void Configure(string interfaceName, int port)
            {
                // In case the beacon was configured twice
                if (_udpSocket != null)
                {
                    _poller.RemovePollInSocket(_udpSocket);
                    _udpSocket.Close();
                }

                _udpPort = port;
                _udpSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

                _poller.AddPollInSocket(_udpSocket, OnUdpReady);

                // Ask operating system for broadcast permissions on socket
                _udpSocket.EnableBroadcast = true;

                // Allow multiple owners to bind to socket; incoming
                // messages will replicate to each owner
                _udpSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

                IPAddress bindTo = null;
                IPAddress sendTo = null;

                if (interfaceName == "*")
                {
                    bindTo = IPAddress.Any;
                    sendTo = IPAddress.Broadcast;
                }
                else
                {
                    var interfaceCollection = new InterfaceCollection();

                    var interfaceAddress = !string.IsNullOrEmpty(interfaceName)
                                               ? IPAddress.Parse(interfaceName)
                                               : null;

                    foreach (var @interface in interfaceCollection)
                    {
                        if (interfaceAddress == null || @interface.Address.Equals(interfaceAddress))
                        {
                            sendTo = @interface.BroadcastAddress;
                            bindTo = @interface.Address;
                            break;
                        }
                    }
                }

                if (bindTo != null)
                {
                    _broadcastAddress = new IPEndPoint(sendTo, _udpPort);
                    _udpSocket.Bind(new IPEndPoint(bindTo, _udpPort));

                    string hostname = string.Empty;

                    try
                    {
                        if (!IPAddress.Any.Equals(bindTo) && !IPAddress.IPv6Any.Equals(bindTo))
                        {
                            hostname = bindTo.ToString();
                        }
                    }
                    catch (Exception)
                    {
                    }

                    _pipe.SendFrame(hostname);
                }
            }

            private void PingElapsed(object sender, NetMQTimerEventArgs e)
            {
                SendUdpFrame(_transmit);
            }

            private void OnUdpReady(Socket socket)
            {
                string peerName;
                var frame = ReceiveUdpFrame(out peerName);

                // If filter is set, check that beacon matches it
                bool isValid = false;
                if (_filter != null)
                {
                    if (frame.MessageSize >= _filter.MessageSize && Compare(frame, _filter, _filter.MessageSize))
                    {
                        isValid = true;
                    }
                }

                // If valid, discard our own broadcasts, which UDP echoes to us
                if (isValid && _transmit != null)
                {
                    if (frame.MessageSize == _transmit.MessageSize && Compare(frame, _transmit, _transmit.MessageSize))
                    {
                        isValid = false;
                    }
                }

                // If still a valid beacon, send on to the API
                if (isValid)
                {
                    _pipe.SendMoreFrame(peerName).SendFrame(frame.Buffer, frame.MessageSize);
                }
            }

            private void OnPipeReady(object sender, NetMQSocketEventArgs e)
            {
                NetMQMessage message = _pipe.ReceiveMultipartMessage();

                string command = message.Pop().ConvertToString();

                switch (command)
                {
                    case ConfigureCommand:
                        string interfaceName = message.Pop().ConvertToString();
                        int port = message.Pop().ConvertToInt32();
                        Configure(interfaceName, port);
                        break;
                    case PublishCommand:
                        _transmit = message.Pop();
                        _pingTimer.Interval = message.Pop().ConvertToInt32();
                        _pingTimer.Enable = true;
                        SendUdpFrame(_transmit);
                        break;
                    case SilenceCommand:
                        _transmit = null;
                        _pingTimer.Enable = false;
                        break;
                    case SubscribeCommand:
                        _filter = message.Pop();
                        break;
                    case UnsubscribeCommand:
                        _filter = null;
                        break;
                    case NetMQActor.EndShimMessage:
                        _poller.Cancel();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            private void SendUdpFrame(NetMQFrame frame)
            {
                _udpSocket.SendTo(frame.Buffer, 0, frame.MessageSize, SocketFlags.None, _broadcastAddress);
            }

            private NetMQFrame ReceiveUdpFrame(out string peerName)
            {
                var buffer = new byte[UdpFrameMax];
                EndPoint peer = new IPEndPoint(IPAddress.Any, 0);

                int bytesRead = _udpSocket.ReceiveFrom(buffer, ref peer);

                var frame = new NetMQFrame(bytesRead);
                Buffer.BlockCopy(buffer, 0, frame.Buffer, 0, bytesRead);

                peerName = peer.ToString();

                return frame;
            }

            #endregion Methods
        }

        #endregion Nested Types
    }
}
