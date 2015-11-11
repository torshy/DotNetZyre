using System;
using System.Linq;
using NetMQ;
using NetMQ.Sockets;

namespace DotNetZyre.Chat
{
    public class Chat : IShimHandler
    {
        #region Fields

        public const string ShoutCommand = "SHOUT";
        public const string WhisperCommand = "WHISPER";
        public const string InterfaceCommand = "INTERFACE";
        public const string StartCommand = "START";

        private readonly NetMQContext _ctx;
        private readonly string _name;

        private PairSocket _pipe;
        private Poller _poller;
        private Zre _zre;

        #endregion Fields

        #region Constructors

        public Chat(NetMQContext ctx, string name)
        {
            _ctx = ctx;
            _name = name;
        }

        #endregion Constructors

        #region Methods

        void IShimHandler.Run(PairSocket pipe)
        {
            _pipe = pipe;
            _pipe.SignalOK();
            _pipe.ReceiveReady += OnPipeReady;

            _zre = Zre.Create(_ctx, _name);
            _zre.ReceiveReady += OnZreReady;
            _zre.SetVerbose();

            _poller = new Poller();
            _poller.AddSocket(_zre);
            _poller.AddSocket(_pipe);
            _poller.PollTillCancelled();
        }

        private void OnZreReady(object sender, ZreEventArgs e)
        {
            var @event = e.Zre.ReceiveEvent();
            if (@event == null)
            {
                return;
            }

            switch (@event.Type)
            {
                case ZreEventType.Enter:
                    Console.WriteLine("* {0} came online", @event.Name);
                    break;
                case ZreEventType.Exit:
                    Console.WriteLine("* {0} left us", @event.Name);
                    break;
                case ZreEventType.Join:
                    Console.WriteLine("* {0} joined {1}", @event.Name, @event.Group);
                    break;
                case ZreEventType.Leave:
                    Console.WriteLine("* {0} left {1}", @event.Name, @event.Group);
                    break;
                case ZreEventType.Whisper:
                    Console.WriteLine("({0}) WHISPERS {1}",
                        @event.Name,
                        string.Join(" ", @event.Message.Select(x => x.ConvertToString())));
                    break;
                case ZreEventType.Shout:
                    Console.WriteLine(
                        "[{0}] ({1}) SHOUTS {2}",
                        @event.Group,
                        @event.Name,
                        string.Join(" ", @event.Message.Select(x => x.ConvertToString())));
                    break;
                case ZreEventType.Stop:
                    Console.WriteLine("{0} quit", @event.Sender);
                    break;
                case ZreEventType.Evasive:
                    Console.WriteLine("{0} is being evasive", @event.Name);
                    break;
            }
        }

        private void OnPipeReady(object sender, NetMQSocketEventArgs e)
        {
            var message = e.Socket.ReceiveMultipartMessage();
            var command = message.Pop().ConvertToString();
            switch (command)
            {
                case ShoutCommand:
                {
                    _zre.Shout("TEST", message.Pop().ConvertToString());
                }
                    break;
                case InterfaceCommand:
                {
                    _zre.SetInterface(message.Pop().ConvertToString());
                }
                    break;
                case StartCommand:
                {
                    _zre.Start();
                    _zre.Join("TEST");
                }
                    break;
            }
        }

        #endregion Methods
    }
}