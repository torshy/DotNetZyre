using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NetMQ;
using NetMQ.Sockets;

namespace DotNetZyre
{
    public class ZreMessage
    {
        #region Fields

        private ZreMessageType _id;
        private ushort _sequence;
        private NetMQFrame _routingId;
        private byte[] _needle;
        private BinaryReader _needleReader;
        private BinaryWriter _needleWriter;
        private long _ceiling;
        private byte _version;
        private string _endpoint;
        private ISet<string> _groups;
        private byte _status;
        private string _name;
        private IDictionary<string, string> _headers;
        private int _headersBytes;
        private NetMQMessage _content;
        private string _group;

        #endregion Fields

        #region Constructors

        public ZreMessage(ZreMessageType id)
        {
            _id = id;
            _groups = new HashSet<string>();
            _headers = new ConcurrentDictionary<string, string>();
        }

        #endregion Constructors

        #region Public Properties

        public ZreMessageType Id
        {
            get
            {
                return _id;
            }
        }

        public NetMQFrame RoutingId
        {
            get
            {
                return _routingId;
            }
        }

        public ushort Sequence
        {
            get
            {
                return _sequence;
            }
            set
            {
                _sequence = value;
            }
        }

        public string Endpoint
        {
            get
            {
                return _endpoint;
            }
            set
            {
                _endpoint = value;
            }
        }

        public ISet<string> Groups
        {
            get
            {
                return _groups;
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

        public string Name
        {
            get
            {
                return _name;
            }
            set
            {
                _name = value;
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

        public NetMQMessage Content
        {
            get
            {
                return _content;
            }
            set
            {
                _content = value;
            }
        }

        public string Group
        {
            get
            {
                return _group;
            }
            set
            {
                _group = value;
            }
        }

        #endregion Public Properties

        #region Public Static Methods

        public static bool IsZreMessage(NetMQMessage message)
        {
            if (message == null)
            {
                return false;
            }

            var frame = message.First;
            var msg = new ZreMessage(0);
            msg._needle = frame.ToByteArray();
            msg._needleReader = new BinaryReader(new MemoryStream(msg._needle));
            msg._needleWriter = new BinaryWriter(new MemoryStream(msg._needle));
            msg._ceiling = msg._needle.Length + frame.MessageSize;
            var signature = msg._needleReader.GetNumber2();
            if (signature != (0xAAA0 | 1))
            {
                return false;
            }

            msg._id = (ZreMessageType)msg._needleReader.GetNumber1();

            switch (msg._id)
            {
                case ZreMessageType.Hello:
                case ZreMessageType.Whisper:
                case ZreMessageType.Shout:
                case ZreMessageType.Join:
                case ZreMessageType.Leave:
                case ZreMessageType.Ping:
                case ZreMessageType.PingOk:
                return true;
            }

            return false;
        }

        public static ZreMessage Decode(NetMQMessage message)
        {
            var msg = new ZreMessage(0);
            var frame = message.Pop();
            if (frame == null)
            {
                return null;
            }

            msg._needle = frame.ToByteArray();
            msg._needleReader = new BinaryReader(new MemoryStream(msg._needle));
            msg._needleWriter = new BinaryWriter(new MemoryStream(msg._needle));
            msg._ceiling = msg._needle.Length + frame.MessageSize;
            var signature = msg._needleReader.GetNumber2();
            if (signature != (0xAAA0 | 1))
            {
                return null;
            }

            msg._id = (ZreMessageType)msg._needleReader.GetNumber1();

            switch (msg._id)
            {
                case ZreMessageType.Hello:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                    msg._endpoint = msg._needleReader.GetString();

                    var listSize = msg._needleReader.GetNumber4();
                    msg._groups = new HashSet<string>();
                    while (listSize-- > 0)
                    {
                        var groupName = msg._needleReader.GetLongString();
                        msg._groups.Add(groupName);
                    }
                    msg._status = msg._needleReader.GetNumber1();
                    msg._name = msg._needleReader.GetString();

                    var hashSize = msg._needleReader.GetNumber4();
                    msg._headers = new ConcurrentDictionary<string, string>();
                    while (hashSize-- > 0)
                    {
                        var key = msg._needleReader.GetString();
                        var value = msg._needleReader.GetLongString();
                        msg._headers.Add(key, value);
                    }
                }
                break;
                case ZreMessageType.Whisper:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                    //  Get zero or more remaining frames, leaving current
                    //  frame untouched
                    msg._content = new NetMQMessage();
                    while (!message.IsEmpty)
                    {
                        msg._content.Append(message.Pop());
                    }

                    if (msg._content.IsEmpty)
                    {
                        msg._content.AppendEmptyFrame();
                    }
                }
                break;
                case ZreMessageType.Shout:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                    msg._group = msg._needleReader.GetString();
                    //  Get zero or more remaining frames, leaving current
                    //  frame untouched
                    msg._content = new NetMQMessage();
                    while (!message.IsEmpty)
                    {
                        msg._content.Append(message.Pop());
                    }

                    if (msg._content.IsEmpty)
                    {
                        msg._content.AppendEmptyFrame();
                    }
                }
                break;
                case ZreMessageType.Join:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                    msg._group = msg._needleReader.GetString();
                    msg._status = msg._needleReader.GetNumber1();
                }
                break;
                case ZreMessageType.Leave:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                    msg._group = msg._needleReader.GetString();
                    msg._status = msg._needleReader.GetNumber1();
                }
                break;
                case ZreMessageType.Ping:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                }
                break;
                case ZreMessageType.PingOk:
                {
                    msg._version = msg._needleReader.GetNumber1();
                    if (msg._version != 2)
                    {
                        return null;
                    }
                    msg._sequence = msg._needleReader.GetNumber2();
                }
                break;
            }

            return msg;
        }

        public static NetMQMessage Encode(ZreMessage msg)
        {
            var message = new NetMQMessage();
            var frameSize = 2 + 1;  //  Signature and message ID

            switch (msg._id)
            {
                case ZreMessageType.Hello:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                //  endpoint is a string with 1-byte length
                frameSize++;       //  Size is one octet
                if (!string.IsNullOrEmpty(msg._endpoint))
                {
                    frameSize += msg._endpoint.Length;
                }
                //  groups is an array of strings
                frameSize += 4;    //  Size is 4 octets
                if (msg._groups != null && msg._groups.Count > 0)
                {
                    //  Add up size of list contents
                    foreach (var @group in msg._groups)
                    {
                        frameSize += 4 + @group.Length;
                    }
                }
                //  status is a 1-byte integer
                frameSize += 1;
                //  name is a string with 1-byte length
                frameSize++;       //  Size is one octet
                if (!string.IsNullOrEmpty(msg._name))
                {
                    frameSize += msg._name.Length;
                }
                //  headers is an array of key=value strings
                frameSize += 4;    //  Size is 4 octets
                if (msg._headers != null && msg._headers.Count > 0)
                {
                    msg._headersBytes = 0;
                    //  Add up size of dictionary contents
                    foreach (var header in msg._headers)
                    {
                        msg._headersBytes += 1 + header.Key.Length;
                        msg._headersBytes += 4 + header.Value.Length;
                    }
                }
                frameSize += msg._headersBytes;
                break;

                case ZreMessageType.Whisper:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                break;

                case ZreMessageType.Shout:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                //  group is a string with 1-byte length
                frameSize++;       //  Size is one octet
                if (!string.IsNullOrEmpty(msg._group))
                {
                    frameSize += msg._group.Length;
                }
                break;

                case ZreMessageType.Join:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                //  group is a string with 1-byte length
                frameSize++;       //  Size is one octet
                if (!string.IsNullOrEmpty(msg._group))
                {
                    frameSize += msg._group.Length;
                }
                //  status is a 1-byte integer
                frameSize += 1;
                break;

                case ZreMessageType.Leave:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                //  group is a string with 1-byte length
                frameSize++;       //  Size is one octet
                if (!string.IsNullOrEmpty(msg._group))
                {
                    frameSize += msg._group.Length;
                }
                //  status is a 1-byte integer
                frameSize += 1;
                break;

                case ZreMessageType.Ping:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                break;

                case ZreMessageType.PingOk:
                //  version is a 1-byte integer
                frameSize += 1;
                //  sequence is a 2-byte integer
                frameSize += 2;
                break;
            }

            var frame = new NetMQFrame(frameSize);
            msg._needle = frame.ToByteArray();
            msg._needleReader = new BinaryReader(new MemoryStream(msg._needle));
            msg._needleWriter = new BinaryWriter(new MemoryStream(msg._needle));
            msg._needleWriter.PutNumber2(0xAAA0 | 1);
            msg._needleWriter.PutNumber1((byte)msg._id);
            switch (msg._id)
            {
                case ZreMessageType.Hello:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                if (!string.IsNullOrEmpty(msg._endpoint))
                {
                    msg._needleWriter.PutString(msg._endpoint);
                }
                else
                {
                    msg._needleWriter.PutNumber1(0);    //  Empty string
                }

                if (msg._groups != null)
                {
                    msg._needleWriter.PutNumber4(msg._groups.Count);
                    foreach (var @group in msg._groups)
                    {
                        msg._needleWriter.PutLongString(@group);
                    }
                }
                else
                {
                    msg._needleWriter.PutNumber4(0);    //  Empty string array
                }

                msg._needleWriter.PutNumber1(msg._status);

                if (!string.IsNullOrEmpty(msg._name))
                {
                    msg._needleWriter.PutString(msg._name);
                }
                else
                {
                    msg._needleWriter.PutNumber1(0);    //  Empty string
                }

                if (msg._headers != null)
                {
                    msg._needleWriter.PutNumber4(msg._headers.Count);
                    foreach (var header in msg._headers)
                    {
                        msg._needleWriter.PutString(header.Key);
                        msg._needleWriter.PutLongString(header.Value);
                    }
                }
                else
                {
                    msg._needleWriter.PutNumber4(0);    //  Empty dictionary
                }
                break;

                case ZreMessageType.Whisper:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                break;

                case ZreMessageType.Shout:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                if (!string.IsNullOrEmpty(msg._group))
                {
                    msg._needleWriter.PutString(msg._group);
                }
                else
                {
                    msg._needleWriter.PutNumber1(0);    //  Empty string
                }
                break;

                case ZreMessageType.Join:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                if (!string.IsNullOrEmpty(msg._group))
                {
                    msg._needleWriter.PutString(msg._group);
                }
                else
                {
                    msg._needleWriter.PutNumber1(0);    //  Empty string
                }
                msg._needleWriter.PutNumber1(msg._status);
                break;

                case ZreMessageType.Leave:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                if (!string.IsNullOrEmpty(msg._group))
                {
                    msg._needleWriter.PutString(msg._group);
                }
                else
                {
                    msg._needleWriter.PutNumber1(0);    //  Empty string
                }
                msg._needleWriter.PutNumber1(msg._status);
                break;

                case ZreMessageType.Ping:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                break;

                case ZreMessageType.PingOk:
                msg._needleWriter.PutNumber1(2);
                msg._needleWriter.PutNumber2(msg._sequence);
                break;
            }

            //  Now send the data frame
            message.Append(frame);

            //  Now send the message field if there is any
            if (msg._id == ZreMessageType.Whisper)
            {
                if (msg._content != null && !msg._content.IsEmpty)
                {
                    while (!msg._content.IsEmpty)
                    {
                        var contentFrame = msg._content.Pop();
                        message.Append(contentFrame);
                    }
                }
                else
                {
                    message.AppendEmptyFrame();
                }
            }
            //  Now send the message field if there is any
            if (msg._id == ZreMessageType.Shout)
            {
                if (msg._content != null && !msg._content.IsEmpty)
                {
                    while (!msg._content.IsEmpty)
                    {
                        var contentFrame = msg._content.Pop();
                        message.Append(contentFrame);
                    }
                }
                else
                {
                    message.AppendEmptyFrame();
                }
            }

            return message;
        }

        public static ZreMessage Receive(IReceivingSocket input)
        {
            NetMQMessage message = input.ReceiveMultipartMessage();
            if (message == null)
            {
                return null;
            }

            NetMQFrame routingId = null;
            if (input is RouterSocket)
            {
                routingId = message.Pop();

                if (routingId == null || message.IsEmpty)
                {
                    return null;
                }
            }

            var zreMsg = Decode(message);
            if (zreMsg != null && input is RouterSocket)
            {
                zreMsg._routingId = routingId;
            }

            return zreMsg;
        }

        #endregion Public Static Methods

        #region Public Methods

        public bool Send(NetMQSocket mailbox)
        {
            var routingId = _routingId;
            _routingId = null;

            var message = Encode(this);
            if (message != null)
            {
                if (mailbox is RouterSocket)
                {
                    message.Push(routingId);
                }

                mailbox.TrySendMultipartMessage(TimeSpan.Zero, message);
                return true;
            }

            return false;
        }

        public ZreMessage Duplicate()
        {
            ZreMessage clone = new ZreMessage(_id);

            if (_routingId != null)
            {
                clone._routingId = _routingId.Duplicate();
            }

            switch (_id)
            {
                case ZreMessageType.Hello:
                clone._version = _version;
                clone._sequence = _sequence;
                clone._endpoint = _endpoint;
                clone._groups = new HashSet<string>(_groups);
                clone._status = _status;
                clone._name = _name;
                clone._headers = new Dictionary<string, string>(_headers);
                break;
                case ZreMessageType.Whisper:
                clone._version = _version;
                clone._sequence = _sequence;
                clone._content = new NetMQMessage(_content.Select(x => x.Duplicate()));
                break;
                case ZreMessageType.Shout:
                clone._version = _version;
                clone._sequence = _sequence;
                clone._group = _group;
                clone._content = new NetMQMessage(_content.Select(x => x.Duplicate()));
                break;
                case ZreMessageType.Join:
                clone._version = _version;
                clone._sequence = _sequence;
                clone._group = _group;
                clone._status = _status;
                break;
                case ZreMessageType.Leave:
                clone._version = _version;
                clone._sequence = _sequence;
                clone._group = _group;
                clone._status = _status;
                break;
                case ZreMessageType.Ping:
                clone._version = _version;
                clone._sequence = _sequence;
                break;
                case ZreMessageType.PingOk:
                clone._version = _version;
                clone._sequence = _sequence;
                break;
            }

            return clone;
        }

        #endregion Public Methods
    }
}
