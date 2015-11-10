using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using NetMQ;
using NetMQ.Sockets;

namespace DotNetZyre
{
    public class ZrePeer
    {
        #region Fields

        private readonly NetMQContext _context;
        private readonly Guid _identity;

        private IDictionary<string, string> _headers;
        private DealerSocket _mailbox;
        private string _name;
        private string _endpoint;
        private string _origin;
        private bool _connected;
        private bool _ready;
        private bool _verbose;
        private ushort _sentSequence;
        private ushort _wantSequence;
        private long _evasiveAt;
        private long _expiredAt;
        private byte _status;

        #endregion Fields

        #region Constructors

        public ZrePeer(NetMQContext context, Guid identity)
        {
            _context = context;
            _identity = identity;
            _headers = new ConcurrentDictionary<string, string>();
            _sentSequence = 0;
            _wantSequence = 0;
            _evasiveAt = 0;
            _expiredAt = 0;
        }

        #endregion Constructors

        #region Public Properties

        public bool Connected
        {
            get
            {
                return _connected;
            }
        }

        public Guid Identity
        {
            get
            {
                return _identity;
            }
        }

        public string Endpoint
        {
            get
            {
                return _endpoint;
            }
        }

        public long EvasiveAt
        {
            get
            {
                return _evasiveAt;
            }
        }

        public long ExpiredAt
        {
            get
            {
                return _expiredAt;
            }
        }

        public string Name
        {
            get
            {
                return _name ?? string.Empty;
            }
            set
            {
                _name = value;
            }
        }

        public string Origin
        {
            get
            {
                return _origin;
            }
            set
            {
                _origin = value;
            }
        }

        public byte Status
        {
            get
            {
                return _status;
            }
            set
            {
                _status = value;
            }
        }

        public bool Ready
        {
            get
            {
                return _ready;
            }
            set
            {
                _ready = value;
            }
        }

        public IDictionary<string, string> Headers
        {
            get
            {
                return _headers;
            }
            set
            {
                _headers = value;
            }
        }

        public bool Verbose
        {
            get
            {
                return _verbose;
            }
            set
            {
                _verbose = value;
            }
        }

        #endregion Public Properties

        #region Public Methods

        public void Connect(Guid from, string endpoint)
        {
            if (_connected)
            {
                throw new InvalidOperationException("Already connected");
            }

            // Set our own identity on the socket so that receiving node
            // knows who each message came from. Note that we cannot use
            // the UUID directly as the identity since it may contain a
            // zero byte at the start, which libzmq does not like for
            // historical and arguably bogus reasons that it nonetheless
            // enforces.
            var identity = new byte[ZreConstants.ZreUuidLength];
            identity[0] = 1;
            Array.Copy(from.ToByteArray(), 0, identity, 1, 16);

            _mailbox = _context.CreateDealerSocket();
            _mailbox.Options.Identity = identity;

            // Set a high-water mark that allows for reasonable activity
            _mailbox.Options.ReceiveHighWatermark = ZreConstants.PeerExpired * 100;
            _mailbox.Options.SendHighWatermark = ZreConstants.PeerExpired * 100;

            try
            {
                _mailbox.Connect(endpoint);
                _endpoint = endpoint;
                _connected = true;
                _ready = false;
            }
            catch (Exception)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) cannot connect to endpoint {1}",
                        _origin,
                        endpoint));
            }

            if (_verbose)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) connected to peer: endpoint {1}",
                        _origin,
                        endpoint));
            }
        }

        public void Disconnect()
        {
            if (_connected)
            {
                _mailbox.Disconnect(_endpoint);
                _mailbox.Dispose();
                _mailbox = null;
                _endpoint = null;
                _ready = false;
                _connected = false;
            }
        }

        public void Refresh()
        {
            _evasiveAt = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + ZreConstants.PeerEvasive;
            _expiredAt = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + ZreConstants.PeerExpired;
        }

        public void Send(ZreMessage msg)
        {
            if (_connected)
            {
                _sentSequence += 1;
                msg.Sequence = _sentSequence;

                if (_verbose)
                {
                    Trace.WriteLine(
                        string.Format(
                            "({0}) send {1} to peer={2} sequence={3}",
                            _origin,
                            msg.Id,
                            _name ?? "-",
                            msg.Sequence));
                }

                try
                {
                    msg.Send(_mailbox);
                }
                catch (Exception)
                {
                    if (_verbose)
                    {
                        Trace.WriteLine(
                            string.Format(
                                "({0}) disconnect from peer (EAGAIN): name={1}",
                                _origin,
                                _name ?? "-"));
                    }
                }
            }
        }

        public bool MessageLost(ZreMessage msg)
        {
            if (_verbose)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) recv {1} from peer={2} sequence={3}",
                        _origin,
                        msg.Id,
                        _name ?? "-",
                        msg.Sequence));
            }

            if (msg.Id == ZreMessageType.Hello)
            {
                _wantSequence = 1;
            }
            else
            {
                _wantSequence += 1;
            }

            if (_wantSequence != msg.Sequence)
            {
                Trace.WriteLine(
                    string.Format(
                        "({0}) seq error from peer={1} expect={2}, got={3}",
                        _origin,
                        _name ?? "-",
                        _wantSequence,
                        msg.Sequence));
                return true;
            }

            return false;
        }

        public string GetHeader(string key, string defaultValue)
        {
            string value;
            if (!_headers.TryGetValue(key, out value))
            {
                return defaultValue;
            }

            return value;
        }

        #endregion Public Methods
    }
}
