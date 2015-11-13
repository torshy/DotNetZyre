using System;
using System.Collections.Generic;
using System.Text;
using NetMQ;

namespace DotNetZyre
{

    public class Zre : IDisposable, ISocketPollable
    {
        #region Fields

        public const string DumpCommand = "DUMP";
        public const string GetNameCommand = "NAME";
        public const string GetOwnGroupsCommand = "OWN GROUPS";
        public const string GetPeerEndpointCommand = "PEER ENDPOINT";
        public const string GetPeerGroupsCommand = "PEER GROUPS";
        public const string GetPeerHeaderCommand = "PEER HEADER";
        public const string GetPeerNameCommand = "PEER NAME";
        public const string GetPeersCommand = "PEERS";
        public const string GetUuidCommand = "UUID";
        public const string GossipBindCommand = "GOSSIP BIND";
        public const string GossipConnectCommand = "GOSSIP CONNECT";
        public const string JoinCommand = "JOIN";
        public const string LeaveCommand = "LEAVE";
        public const string SetEndPointCommand = "SET ENDPOINT";
        public const string SetHeaderCommand = "SET HEADER";
        public const string SetInterfaceCommand = "SET INTERFACE";
        public const string SetIntervalCommand = "SET INTERVAL";
        public const string SetNameCommand = "SET NAME";
        public const string SetPortCommand = "SET PORT";
        public const string SetUuidCommand = "SET UUID";
        public const string SetVerboseCommand = "SET VERBOSE";
        public const string ShoutCommand = "SHOUT";
        public const string StartCommand = "START";
        public const string StopCommand = "STOP";
        public const string TerminateCommand = "$TERM";
        public const string WhisperCommand = "WHISPER";

        private readonly NetMQActor _actor;
        private readonly EventDelegator<ZreEventArgs> _receiveEvent;

        private string _name;
        private Guid _uuid;

        #endregion Fields

        #region Constructors

        private Zre(NetMQContext context, string name)
        {
            _name = name;
            _actor = NetMQActor.Create(context, new ZreNode(context));
            if (!string.IsNullOrEmpty(name))
            {
                Name = name;
            }

            EventHandler<NetMQActorEventArgs> onReceive = (sender, e) =>
                                              _receiveEvent.Fire(this, new ZreEventArgs(this));

            _receiveEvent = new EventDelegator<ZreEventArgs>(
                () => _actor.ReceiveReady += onReceive,
                () => _actor.ReceiveReady -= onReceive);
        }

        #endregion Constructors

        #region Events

        /// <summary>
        /// This event occurs when at least one message may be received from the socket without blocking.
        /// </summary>
        public event EventHandler<ZreEventArgs> ReceiveReady
        {
            add
            {
                _receiveEvent.Event += value;
            }
            remove
            {
                _receiveEvent.Event -= value;
            }
        }

        #endregion Events

        #region Properties

        public string Name
        {
            get
            {
                if (string.IsNullOrEmpty(_name))
                {
                    if (_actor.TrySendFrame(GetNameCommand))
                    {
                        _name = _actor.ReceiveFrameString(Encoding.UTF8);
                    }
                }

                return _name;
            }
            set
            {
                _actor
                    .SendMoreFrame(SetNameCommand)
                    .SendFrame(value);
            }
        }

        public Guid Identity
        {
            get
            {
                if (_uuid == Guid.Empty)
                {
                    if (_actor.TrySendFrame(GetUuidCommand))
                    {
                        _uuid = new Guid(_actor.ReceiveFrameString());
                    }
                }

                return _uuid;
            }
            set
            {
                _actor
                    .SendMoreFrame(SetUuidCommand)
                    .SendFrame(value.ToByteArray());
            }
        }

        NetMQSocket ISocketPollable.Socket
        {
            get
            {
                return ((ISocketPollable)_actor).Socket;
            }
        }

        #endregion Properties

        #region Methods

        public static Zre Create(NetMQContext context, string name)
        {
            return new Zre(context, name);
        }

        public bool Start()
        {
            _actor.SendFrame(StartCommand);
            return _actor.ReceiveSignal();
        }

        public void Stop()
        {
            _actor.SendFrame(StopCommand);
            _actor.ReceiveSignal();
        }

        public void SetVerbose()
        {
            _actor.SendFrame(SetVerboseCommand);
        }

        public void SetInterval(int interval)
        {
            _actor
                .SendMoreFrame(SetIntervalCommand)
                .SendFrame(Convert.ToString(interval));
        }

        public void SetInterface(string interfaceName)
        {
            _actor
                .SendMoreFrame(SetInterfaceCommand)
                .SendFrame(interfaceName);
        }

        public void Join(string group)
        {
            _actor
                .SendMoreFrame(JoinCommand)
                .SendFrame(group);
        }

        public void Leave(string group)
        {
            _actor
                .SendMoreFrame(LeaveCommand)
                .SendFrame(group);
        }

        public void Whisper(Guid peerIdentity, NetMQMessage message)
        {
            _actor
                .SendMoreFrame(WhisperCommand)
                .SendMoreFrame(peerIdentity.ToString())
                .SendMultipartMessage(message);
        }

        public void Whisper(Guid peerIdentity, string format, params object[] args)
        {
            var message = args != null && args.Length > 0 ? string.Format(format, args) : format;
            _actor
                .SendMoreFrame(WhisperCommand)
                .SendMoreFrame(peerIdentity.ToString())
                .SendFrame(message);
        }

        public void Shout(string group, NetMQMessage message)
        {
            _actor
                .SendMoreFrame(ShoutCommand)
                .SendMoreFrame(group)
                .SendMultipartMessage(message);
        }

        public void Shout(string group, string format, params object[] args)
        {
            var message = args != null && args.Length > 0 ? string.Format(format, args) : format;
            _actor
                .SendMoreFrame(ShoutCommand)
                .SendMoreFrame(group)
                .SendFrame(message);
        }

        public IEnumerable<string> Peers()
        {
            _actor.SendFrame(GetPeersCommand);
            var peers = new List<string>();
            var msg = _actor.ReceiveMultipartMessage();
            while (!msg.IsEmpty)
            {
                peers.Add(msg.Pop().ConvertToString());
            }

            return peers;
        }

        public void SetHeader(string key, string value)
        {
            _actor
                .SendMoreFrame(SetHeaderCommand)
                .SendMoreFrame(key)
                .SendFrame(value);
        }

        public string GetHeader(Guid peerIdentity, string key)
        {
            _actor
                .SendMoreFrame(GetPeerHeaderCommand)
                .SendMoreFrame(peerIdentity.ToString())
                .SendFrame(key);

            return _actor.ReceiveFrameString();
        }

        public ZreEvent ReceiveEvent()
        {
            return ZreEvent.Create(_actor.ReceiveMultipartMessage());
        }

        public void Dispose()
        {
            _actor.SendFrame(StopCommand);
            _actor.Dispose();
        }

        #endregion Methods
    }
}