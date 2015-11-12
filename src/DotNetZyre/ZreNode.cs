using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using NetMQ;
using NetMQ.Sockets;

namespace DotNetZyre
{
    public class ZreNode : IShimHandler
    {
        #region Fields

        private readonly NetMQContext _context;
        private readonly ConcurrentDictionary<string, ZreGroup> _ownGroups;     // Groups that we are in
        private readonly ConcurrentDictionary<string, ZreGroup> _peerGroups;    // Groups that our peers are in
        private readonly ConcurrentDictionary<Guid, ZrePeer> _peers;            // Hash of known peers, fast lookup
        private readonly ConcurrentDictionary<string, string> _headers;         // Our header values

        private NetMQActor _gossip;
        private string _gossipBind;
        private string _gossipConnect;
        private ZreBeacon _beacon;
        private RouterSocket _inbox;            // Our inbox socket (ROUTER)
        private PairSocket _pipe;
        private Poller _poller;
        private NetMQTimer _timer;
        private string _name;                   // Our public name
        private TimeSpan _interval;             // Beacon interval
        private int _beaconPort;                // Beacon port number
        private string _beaconInterfaceName;    // Becon interface name
        private int _port;                      // Our inbox port number
        private byte _status;                   // Our own change counter
        private Guid _identity;                 // Our Identity
        private bool _verbose;                  // Log all traffic
        private string _endpoint;               // Our public endpoint

        #endregion Fields

        #region Constructors

        public ZreNode(NetMQContext context)
        {
            _context = context;

            _identity = Guid.NewGuid();
            _name = _identity.ToString().Substring(0, 6);
            _interval = TimeSpan.FromSeconds(1);
            _beaconPort = ZreConstants.ZreDiscoveryPort;

            _headers = new ConcurrentDictionary<string, string>();
            _ownGroups = new ConcurrentDictionary<string, ZreGroup>();
            _peerGroups = new ConcurrentDictionary<string, ZreGroup>();
            _peers = new ConcurrentDictionary<Guid, ZrePeer>();
        }

        #endregion Constructors

        #region Public Methods

        void IShimHandler.Run(PairSocket shim)
        {
            _pipe = shim;
            _pipe.SignalOK();
            _pipe.ReceiveReady += OnPipeReady;

            _timer = new NetMQTimer(TimeSpan.FromSeconds(1));
            _timer.Elapsed += OnPingPeer;

            _inbox = _context.CreateRouterSocket();
            _inbox.ReceiveReady += OnInboxReady;

            _poller = new Poller(_pipe);
            _poller.AddTimer(_timer);
            _poller.PollTillCancelled();
        }

        #endregion Public Methods

        #region Private Methods

        private bool Start()
        {
            if (_beaconPort > 0)
            {
                // Start beacon discovery
                _beacon = new ZreBeacon(_context);
                _beacon.ReceiveReady += OnBeaconReady;
                if (string.IsNullOrEmpty(_beaconInterfaceName) || _beaconInterfaceName == "*")
                {
                    _beacon.Configure(_beaconPort);
                }
                else
                {
                    _beacon.Configure(_beaconInterfaceName, _beaconPort);
                }

                var hostname = _beacon.Hostname;
                var address = string.Format("tcp://{0}", hostname);
                _port = _inbox.BindRandomPort(address);
                _endpoint = string.Format("{0}:{1}", address, _port);

                if (_verbose)
                {
                    Trace.WriteLine(
                        string.Format(
                            "({0}) beaconing port={1} interface={2} host={3}",
                            _name,
                            _port,
                            _beaconInterfaceName,
                            hostname));
                }

                var beacon = new Beacon();
                beacon.Protocol = new[] { 'Z', 'R', 'E' };
                beacon.Version = ZreConstants.BeaconVersion;
                beacon.Port = (ushort)_port;
                beacon.Identity = _identity;

                var beaconData = beacon.Serialize();
                _beacon.Publish(beaconData, _interval);
                _beacon.Subscribe("ZRE");
                _poller.AddSocket(_beacon);
            }
            else
            {
                //  Start gossip discovery
                //  ------------------------------------------------------------------
                //  If application didn't set an endpoint explicitly, grab ephemeral
                //  port on all available network interfaces.
            }

            _poller.AddSocket(_inbox);

            return true;
        }

        private bool Stop()
        {
            if (_beacon != null)
            {
                //  Stop broadcast/listen beacon

                var beacon = new Beacon();
                beacon.Protocol = new[] { 'Z', 'R', 'E' };
                beacon.Version = ZreConstants.BeaconVersion;
                beacon.Port = 0;
                beacon.Identity = _identity;

                _beacon.Publish(beacon.Serialize(), _interval);
                _poller.RemoveSocket(_beacon);
                _beacon.ReceiveReady -= OnBeaconReady;
                _beacon.Dispose();
                _beacon = null;
            }

            _poller.RemoveSocket(_inbox);

            return true;
        }

        private void StartGossip()
        {
            //  If we haven't already set-up the gossip network, do so
            if (_gossip == null)
            {
                _beaconPort = 0; //  Disable UDP beaconing
                _gossip = NetMQActor.Create(
                    _context,
                    shim =>
                    {
                    });

                if (_verbose)
                {
                    _gossip.SendFrame(Zre.SetVerboseCommand);
                }
            }
        }

        private void OnPingPeer(object sender, NetMQTimerEventArgs e)
        {
            foreach (var peer in _peers.Values)
            {
                var clockTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                if (clockTime >= peer.ExpiredAt)
                {
                    if (_verbose)
                    {
                        Trace.WriteLine(
                            string.Format(
                                "({0}) peer expired name={1} endpoint={2}",
                                _name,
                                peer.Name,
                                peer.Endpoint));
                    }

                    RemovePeer(peer);
                }
                else if (clockTime >= peer.EvasiveAt)
                {
                    //  If peer is being evasive, force a TCP ping.
                    //  TODO: do this only once for a peer in this state;
                    //  it would be nicer to use a proper state machine
                    //  for peer management.
                    if (_verbose)
                    {
                        Trace.WriteLine(
                            string.Format(
                                "({0}) peer seems dead/slow name={1} endpoint={2}",
                                _name,
                                peer.Name,
                                peer.Endpoint));
                    }

                    var ping = new ZreMessage(ZreMessageType.Ping);
                    peer.Send(ping);

                    // Inform the calling application this peer is being evasive
                    _pipe
                        .SendMoreFrame("EVASIVE")
                        .SendMoreFrame(peer.Identity.ToString())
                        .SendFrame(peer.Name);
                }
            }
        }

        private void OnBeaconReady(object sender, ZreBeaconEventArgs e)
        {
            string peerName;
            var data = e.Beacon.Receive(out peerName);
            if (data.Length != Beacon.BeaconSize() || string.IsNullOrEmpty(peerName))
            {
                return;
            }

            var beacon = Beacon.Deserialize(data);
            if (beacon.Version != ZreConstants.BeaconVersion)
            {
                return;
            }

            var portIndex = peerName.IndexOf(":", StringComparison.Ordinal);
            portIndex = portIndex == -1 ? peerName.Length : portIndex;
            var ipAddress = peerName.Substring(0, portIndex);

            if (beacon.Port > 0)
            {
                var endpoint = string.Format("tcp://{0}:{1}", ipAddress, beacon.Port);
                var peer = RequirePeer(beacon.Identity, endpoint);
                peer.Refresh();
            }
            else
            {
                //  Zero port means peer is going away; remove it if
                //  we had any knowledge of it already
                ZrePeer peer;
                _peers.TryRemove(beacon.Identity, out peer);
            }
        }

        private void OnInboxReady(object sender, NetMQSocketEventArgs e)
        {
            var msg = ZreMessage.Receive(e.Socket);
            if (msg == null)
            {
                return;
            }

            if (msg.RoutingId.MessageSize != ZreConstants.ZreUuidLength)
            {
                return;
            }

            // Router socket tells us the identity of this peer
            // Identity must be [1] followed by 16-byte UUID, ignore the [1]
            var uuidBuffer = new byte[16];
            var uuidReceived = msg.RoutingId.ToByteArray();
            Array.Copy(uuidReceived, 1, uuidBuffer, 0, uuidBuffer.Length);
            var senderId = new Guid(uuidBuffer);

            //  On HELLO we may create the peer if it's unknown
            //  On other commands the peer must already exist
            ZrePeer peer;
            _peers.TryGetValue(senderId, out peer);
            if (msg.Id == ZreMessageType.Hello)
            {
                if (peer != null)
                {
                    //  Remove fake peers
                    if (peer.Ready)
                    {
                        RemovePeer(peer);
                    }
                    else if (peer.Endpoint == _endpoint)
                    {
                        //  We ignore HELLO, if peer has same endpoint as current node
                        return;
                    }
                }

                peer = RequirePeer(senderId, msg.Endpoint);
                peer.Ready = true;
            }

            if (peer == null || !peer.Ready)
            {
                return;
            }

            if (peer.MessageLost(msg))
            {
                Trace.WriteLine(string.Format("({0}) messages lost from {1}", _name, peer.Name));
                RemovePeer(peer);
                return;
            }

            //  Now process each command
            if (msg.Id == ZreMessageType.Hello)
            {
                //  Store properties from HELLO command into peer
                peer.Name = msg.Name;
                peer.Headers = msg.Headers;

                //  Tell the caller about the peer
                _pipe
                    .SendMoreFrame("ENTER")
                    .SendMoreFrame(peer.Identity.ToString())
                    .SendMoreFrame(peer.Name)
                    .SendMoreFrame(peer.Headers.PackHeaders())
                    .SendFrame(msg.Endpoint);

                if (_verbose)
                {
                    Trace.WriteLine(
                        string.Format(
                            "({0}) ENTER name={1} endpoint={2}",
                            _name,
                            peer.Name,
                            peer.Endpoint));
                }

                //  Join peer to listed groups
                foreach (var @group in msg.Groups)
                {
                    JoinPeerGroup(peer, @group);
                }

                //  Now take peer's status from HELLO, after joining groups
                peer.Status = msg.Status;
            }
            else if (msg.Id == ZreMessageType.Whisper)
            {
                //  Pass up to caller API as WHISPER event
                _pipe
                    .SendMoreFrame(Zre.WhisperCommand)
                    .SendMoreFrame(senderId.ToString())
                    .SendMoreFrame(peer.Name)
                    .SendMultipartMessage(msg.Content);
            }
            else if (msg.Id == ZreMessageType.Shout)
            {
                //  Pass up to caller API as SHOUT event
                _pipe
                    .SendMoreFrame(Zre.ShoutCommand)
                    .SendMoreFrame(senderId.ToString())
                    .SendMoreFrame(peer.Name)
                    .SendMoreFrame(msg.Group)
                    .SendMultipartMessage(msg.Content);
            }
            else if (msg.Id == ZreMessageType.Ping)
            {
                var pong = new ZreMessage(ZreMessageType.PingOk);
                peer.Send(pong);
            }
            else if (msg.Id == ZreMessageType.Join)
            {
                JoinPeerGroup(peer, msg.Group);
            }
            else if (msg.Id == ZreMessageType.Leave)
            {
                LeavePeerGroup(peer, msg.Group);
            }

            //  Activity from peer resets peer timers
            peer.Refresh();
        }

        private void OnPipeReady(object sender, NetMQSocketEventArgs e)
        {
            //  Get the whole message off the pipe in one go
            var message = e.Socket.ReceiveMultipartMessage();
            if (message == null)
            {
                return;
            }

            var command = message.Pop().ConvertToString();
            switch (command)
            {
                case Zre.StartCommand:
                if (Start())
                {
                    e.Socket.SignalOK();
                }
                else
                {
                    e.Socket.SignalError();
                }
                break;
                case Zre.StopCommand:
                if (Stop())
                {
                    e.Socket.SignalOK();
                }
                else
                {
                    e.Socket.SignalError();
                }
                break;
                case Zre.SetNameCommand:
                _name = message.Pop().ConvertToString();
                break;
                case Zre.GetNameCommand:
                _pipe.SendFrame(_name);
                break;
                case Zre.SetUuidCommand:
                _identity = new Guid(message.Pop().ToByteArray());
                break;
                case Zre.GetUuidCommand:
                _pipe.SendFrame(_identity.ToString());
                break;
                case Zre.SetIntervalCommand:
                _interval = TimeSpan.FromMilliseconds(message.Pop().ConvertToInt32());
                break;
                case Zre.SetVerboseCommand:
                _verbose = true;
                break;
                case Zre.SetHeaderCommand:
                {
                    var key = message.Pop().ConvertToString();
                    var value = message.Pop().ConvertToString();
                    _headers[key] = value;
                }
                break;
                case Zre.SetPortCommand:
                _beaconPort = message.Pop().ConvertToInt32();
                break;
                case Zre.WhisperCommand:
                {
                    //  Get peer to send message to
                    var identityString = message.Pop().ConvertToString();
                    Guid identity;
                    if (Guid.TryParse(identityString, out identity))
                    {
                        //  Send frame on out to peer's mailbox, drop message
                        //  if peer doesn't exist (may have been destroyed)
                        ZrePeer peer;
                        if (_peers.TryGetValue(identity, out peer))
                        {
                            var msg = new ZreMessage(ZreMessageType.Whisper);
                            msg.Content = message;
                            peer.Send(msg);
                        }
                    }
                }
                break;
                case Zre.ShoutCommand:
                {
                    //  Get group to send message to
                    var groupName = message.Pop().ConvertToString();
                    ZreGroup group;
                    if (_peerGroups.TryGetValue(groupName, out group))
                    {
                        var msg = new ZreMessage(ZreMessageType.Shout);
                        msg.Group = groupName;
                        msg.Content = message;
                        group.Send(msg);
                    }
                }
                break;
                case Zre.JoinCommand:
                {
                    var groupName = message.Pop().ConvertToString();
                    ZreGroup @group;
                    if (!_ownGroups.TryGetValue(groupName, out group))
                    {
                        @group = new ZreGroup(groupName);
                        _ownGroups[groupName] = @group;
                        var joinMsg = new ZreMessage(ZreMessageType.Join);
                        joinMsg.Group = groupName;
                        joinMsg.Status = ++_status;
                        foreach (var zrePeer in _peers)
                        {
                            zrePeer.Value.Send(joinMsg.Duplicate());
                        }

                        if (_verbose)
                        {
                            Trace.WriteLine(string.Format("({0}) JOIN group={1}", _name, groupName));
                        }
                    }
                }
                break;
                case Zre.LeaveCommand:
                {
                    var groupName = message.Pop().ConvertToString();
                    ZreGroup @group;
                    if (_ownGroups.TryGetValue(groupName, out group))
                    {
                        var leaveMsg = new ZreMessage(ZreMessageType.Leave);
                        leaveMsg.Group = groupName;
                        leaveMsg.Status = ++_status;
                        foreach (var zrePeer in _peers)
                        {
                            zrePeer.Value.Send(leaveMsg.Duplicate());
                        }

                        _ownGroups.TryRemove(groupName, out @group);

                        if (_verbose)
                        {
                            Trace.WriteLine(string.Format("({0}) LEAVE group={1}", _name, groupName));
                        }
                    }
                }
                break;
                case Zre.GetPeersCommand:
                {
                    var peerNames = _peers.Keys.ToArray();

                    using (var msgStream = new MemoryStream())
                    using (var writer = new BinaryWriter(msgStream))
                    {
                        foreach (var peerName in peerNames)
                        {
                            writer.PutString(peerName.ToString());
                        }

                        _pipe.SendFrame(msgStream.ToArray());
                    }
                }
                break;
                case Zre.GetPeerGroupsCommand:
                {
                }
                break;
                case Zre.GetOwnGroupsCommand:
                {
                }
                break;
                case Zre.GetPeerEndpointCommand:
                {
                    var identityString = message.Pop().ConvertToString();
                    Guid identity;
                    if (Guid.TryParse(identityString, out identity))
                    {
                        ZrePeer peer;
                        if (_peers.TryGetValue(identity, out peer))
                        {
                            _pipe.SendFrame(peer.Endpoint);
                        }
                    }
                }
                break;
                case Zre.GetPeerHeaderCommand:
                {
                    var identityString = message.Pop().ConvertToString();
                    var key = message.Pop().ConvertToString();
                    Guid identity;
                    if (Guid.TryParse(identityString, out identity))
                    {
                        ZrePeer peer;
                        if (_peers.TryGetValue(identity, out peer))
                        {
                            var header = peer.GetHeader(key, defaultValue: string.Empty);
                            _pipe.SendFrame(header);
                        }
                        else
                        {
                            _pipe.SendFrame(string.Empty);
                        }
                    }
                }
                break;
                case Zre.SetEndPointCommand:
                {
                    StartGossip();
                    var endpoint = message.Pop().ConvertToString();
                    try
                    {
                        _inbox.Bind(endpoint);
                        _endpoint = endpoint;
                        _pipe.SignalOK();
                    }
                    catch (Exception)
                    {
                        _pipe.SignalError();
                    }
                }
                break;
                case Zre.GossipBindCommand:
                {
                    StartGossip();
                    _gossipBind = message.Pop().ConvertToString();
                    _gossip.SendMoreFrame("BIND").SendFrame(_gossipBind);
                }
                break;
                case Zre.GossipConnectCommand:
                {
                    StartGossip();
                    _gossipConnect = message.Pop().ConvertToString();
                    _gossip.SendMoreFrame("CONNECT").SendFrame(_gossipConnect);
                }
                break;
                case Zre.SetInterfaceCommand:
                {
                    _beaconInterfaceName = message.Pop().ConvertToString();
                }
                break;
                case NetMQActor.EndShimMessage:
                _poller.Cancel();
                break;
            }
        }

        private ZreGroup JoinPeerGroup(ZrePeer peer, string groupName)
        {
            var group = RequirePeerGroup(groupName);
            group.Join(peer);

            //  Now tell the caller about the peer joined group
            _pipe
                .SendMoreFrame("JOIN")
                .SendMoreFrame(peer.Identity.ToString())
                .SendMoreFrame(peer.Name)
                .SendFrame(groupName);

            if (_verbose)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) JOIN name={1} group={2}",
                        _name,
                        peer.Name,
                        groupName));
            }

            return group;
        }

        private ZreGroup LeavePeerGroup(ZrePeer peer, string groupName)
        {
            var group = RequirePeerGroup(groupName);
            @group.Leave(peer);

            //  Now tell the caller about the peer left group
            _pipe
                .SendMoreFrame("LEAVE")
                .SendMoreFrame(peer.Identity.ToString())
                .SendMoreFrame(peer.Name)
                .SendFrame(groupName);

            if (_verbose)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) LEAVE name={1} group={2}",
                        _name,
                        peer.Name,
                        groupName));
            }

            return group;
        }

        private void RemovePeer(ZrePeer peer)
        {
            //  Tell the calling application the peer has gone
            _pipe
                .SendMoreFrame("EXIT")
                .SendMoreFrame(peer.Identity.ToString())
                .SendFrame(peer.Name);

            if (_verbose)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) EXIT name={1} endpoint={2}",
                        _name,
                        peer.Name,
                        peer.Endpoint));
            }

            //  Remove peer from any groups we've got it in
            foreach (var peerGroup in _peerGroups)
            {
                peerGroup.Value.Leave(peer);
            }

            //  To destroy peer, we remove from peers hash table
            _peers.TryRemove(peer.Identity, out peer);
        }

        private ZreGroup RequirePeerGroup(string groupName)
        {
            return _peerGroups.GetOrAdd(groupName, name => new ZreGroup(name));
        }

        private ZrePeer RequirePeer(Guid identity, string endpoint)
        {
            ZrePeer peer;
            if (_peers.TryGetValue(identity, out peer))
            {
                return peer;
            }

            //  Purge any previous peer on same endpoint
            PurgePeer(endpoint);

            peer = new ZrePeer(_context, identity);
            _peers.TryAdd(identity, peer);
            peer.Origin = _name;
            peer.Verbose = _verbose;
            peer.Connect(_identity, endpoint);

            // Handshake discovery by sending HELLO as first message
            var groups = _ownGroups.Keys.ToArray();
            var headers = new ConcurrentDictionary<string, string>(_headers);
            var msg = new ZreMessage(ZreMessageType.Hello);
            msg.Endpoint = _endpoint;
            msg.Groups.UnionWith(groups);
            msg.Status = _status;
            msg.Name = _name;
            msg.Headers = headers;
            peer.Send(msg);

            return peer;
        }

        private void PurgePeer(string endpoint)
        {
            foreach (var kvp in _peers)
            {
                if (kvp.Value.Endpoint == endpoint)
                {
                    kvp.Value.Disconnect();
                }
            }
        }

        #endregion Private Methods

        #region Nested Types

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        private struct Beacon
        {
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 3)]
            public char[] Protocol;
            public byte Version;
            public Guid Identity;
            public ushort Port;

            #region Public Static Methods

            public static Beacon Deserialize(byte[] data)
            {
                var beaconSize = BeaconSize();
                if (beaconSize != data.Length)
                {
                    return new Beacon();
                }

                using (var reader = new BinaryReader(new MemoryStream(data)))
                {
                    // Read in a byte array
                    byte[] bytes = reader.ReadBytes(beaconSize);

                    // Pin the managed memory while, copy it out the data, then unpin it
                    GCHandle handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                    Beacon theStructure = (Beacon)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(Beacon));
                    handle.Free();

                    return theStructure;
                }
            }

            public static int BeaconSize()
            {
                return Marshal.SizeOf(typeof(Beacon));
            }

            #endregion Public Static Methods

            #region Public Methods

            public byte[] Serialize()
            {
                int size = Marshal.SizeOf(this);
                byte[] arr = new byte[size];

                IntPtr ptr = Marshal.AllocHGlobal(size);
                Marshal.StructureToPtr(this, ptr, true);
                Marshal.Copy(ptr, arr, 0, size);
                Marshal.FreeHGlobal(ptr);
                return arr;
            }

            #endregion Public Methods
        }

        #endregion Nested Types
    }
}
