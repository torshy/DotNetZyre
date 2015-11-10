using System;

namespace DotNetZyre
{
    public class ZreEventArgs : EventArgs
    {
        /// <summary>
        /// Create a new ZreEventArgs object containing the given Zre.
        /// </summary>
        public ZreEventArgs(Zre zre)
        {
            Zre = zre;
        }

        /// <summary>
        /// Get the Zre object that this holds.
        /// </summary>
        public Zre Zre
        {
            get; private set;
        }
    }
}