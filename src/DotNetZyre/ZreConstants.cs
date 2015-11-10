namespace DotNetZyre
{
    internal class ZreConstants
    {
        #region Fields

        public static readonly byte BeaconVersion = 0x1;
        public static readonly int DynamicPortFrom = 0xc000;
        public static readonly int DynamicPortTo = 0xffff;

        //  Private constants
        public static readonly int PeerEvasive = 10000;     // 10 seconds' silence is evasive
        public static readonly int PeerExpired = 30000;     // 30 seconds' silence is expired
        public static readonly int ReapetInterval = 1000;   // Once per second

        //  IANA-assigned port for ZYRE discovery protocol
        public static readonly int ZreDiscoveryPort = 5670;
        public static readonly int ZreUuidLength = 16 + 1;

        #endregion Fields
    }
}
