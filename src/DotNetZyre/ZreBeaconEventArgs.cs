using System;

namespace DotNetZyre
{
    /// <summary>
    /// A ZreBeaconEventArgs is an EventArgs that provides a property that holds a ZreBeacon.
    /// </summary>
    public class ZreBeaconEventArgs : EventArgs
    {
        /// <summary>
        /// Create a new ZreBeaconEventArgs object containing the given ZreBeacon.
        /// </summary>
        /// <param name="beacon">the ZreBeacon object to hold a reference to</param>
        public ZreBeaconEventArgs(ZreBeacon beacon)
        {
            Beacon = beacon;
        }

        /// <summary>
        /// Get the ZreBeacon object that this holds.
        /// </summary>
        public ZreBeacon Beacon { get; private set; }
    }
}
