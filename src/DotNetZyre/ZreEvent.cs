using System.Collections.Generic;
using NetMQ;

namespace DotNetZyre
{
    public class ZreEvent
    {
        #region Fields

        private string _address;
        private string _group;
        private IDictionary<string, string> _headers;
        private NetMQMessage _message;
        private string _name;
        private string _sender;
        private ZreEventType _type;

        #endregion Fields

        #region Properties

        public ZreEventType Type
        {
            get
            {
                return _type;
            }
            set
            {
                _type = value;
            }
        }

        public string Sender
        {
            get
            {
                return _sender;
            }
            set
            {
                _sender = value;
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

        public string Address
        {
            get
            {
                return _address;
            }
            set
            {
                _address = value;
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

        public NetMQMessage Message
        {
            get
            {
                return _message;
            }
            set
            {
                _message = value;
            }
        }

        #endregion Properties

        #region Methods

        public static ZreEvent Create(NetMQMessage message)
        {
            if (message == null)
            {
                return null;
            }

            var self = new ZreEvent();
            var type = message.Pop().ConvertToString();
            self.Sender = message.Pop().ConvertToString();
            self.Name = message.Pop().ConvertToString();

            switch (type)
            {
                case "ENTER":
                self.Type = ZreEventType.Enter;
                var headersFrame = message.Pop();
                if (headersFrame != null)
                {
                    var headers = headersFrame.ToByteArray().UnpackHeaders();
                    self.Headers = headers;
                }
                self.Address = message.Pop().ConvertToString();
                break;
                case "EXIT":
                self.Type = ZreEventType.Exit;
                break;
                case "JOIN":
                self.Type = ZreEventType.Join;
                self.Group = message.Pop().ConvertToString();
                break;
                case "LEAVE":
                self.Type = ZreEventType.Leave;
                self.Group = message.Pop().ConvertToString();
                break;
                case "WHISPER":
                self.Type = ZreEventType.Whisper;
                self.Message = message;
                break;
                case "SHOUT":
                self.Type = ZreEventType.Shout;
                self.Group = message.Pop().ConvertToString();
                self.Message = message;
                break;
                case "STOP":
                self.Type = ZreEventType.Stop;
                break;
            }

            return self;
        }

        #endregion Methods
    }
}
