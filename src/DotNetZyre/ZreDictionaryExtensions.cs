using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DotNetZyre
{
    internal static class ZreDictionaryExtensions
    {
        public static byte[] PackHeaders(this IDictionary<string, string> headers)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                foreach (var header in headers)
                {
                    writer.Write((short)header.Key.Length);
                    writer.Write(header.Key);
                    writer.Write(header.Value.Length);
                    writer.Write(header.Value);
                }

                return stream.ToArray();
            }
        }

        public static IDictionary<string, string> UnpackHeaders(this byte[] data)
        {
            var headers = new Dictionary<string, string>();
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReader(stream))
            {
                while (stream.Position < stream.Length)
                {
                    var keylength = reader.ReadInt16();
                    var key = Encoding.UTF8.GetString(reader.ReadBytes(keylength));
                    var valueLength = reader.ReadInt32();
                    var value = Encoding.UTF8.GetString(reader.ReadBytes(valueLength));
                    headers.Add(key, value);
                }
            }

            return headers;
        }
    }
}
