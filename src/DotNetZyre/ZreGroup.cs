using System;
using System.Collections.Concurrent;

namespace DotNetZyre
{
    public class ZreGroup
    {
        #region Fields

        private readonly string _name;
        private readonly ConcurrentDictionary<Guid, ZrePeer> _peers;

        #endregion Fields

        #region Constructors

        public ZreGroup(string name)
        {
            _name = name;
            _peers = new ConcurrentDictionary<Guid, ZrePeer>();
        }

        #endregion Constructors

        #region Public Properties

        public string Name
        {
            get
            {
                return _name;
            }
        }

        #endregion Public Properties

        #region Public Methods

        public void Join(ZrePeer peer)
        {
            peer = _peers.GetOrAdd(peer.Identity, guid => peer);
            peer.Status += 1;
        }

        public void Leave(ZrePeer peer)
        {
            ZrePeer tmp;
            if (_peers.TryRemove(peer.Identity, out tmp))
            {
                peer.Status += 1;
            }
        }

        public void Send(ZreMessage msg)
        {
            foreach (var peer in _peers)
            {
                peer.Value.Send(msg.Duplicate());
            }
        }

        #endregion Public Methods
    }
}
