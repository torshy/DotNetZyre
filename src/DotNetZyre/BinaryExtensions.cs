using System.IO;
using System.Text;

namespace DotNetZyre
{
    internal static class BinaryExtensions
    {
        public static byte GetNumber1(this BinaryReader reader)
        {
            var value = reader.ReadByte();
            return value;
        }

        public static void PutNumber1(this BinaryWriter writer, byte value)
        {
            writer.Write(value);
        }

        public static ushort GetNumber2(this BinaryReader reader)
        {
            return reader.ReadUInt16();
        }

        public static void PutNumber2(this BinaryWriter writer, int value)
        {
            writer.Write((ushort)value);
        }

        public static int GetNumber4(this BinaryReader reader)
        {
            return reader.ReadInt32();
        }

        public static void PutNumber4(this BinaryWriter writer, int value)
        {
            writer.Write(value);
        }

        public static long GetNumber8(this BinaryReader reader)
        {
            return reader.ReadInt64();
        }

        public static void PutString(this BinaryWriter writer, string value)
        {
            PutNumber1(writer, (byte)Encoding.UTF8.GetByteCount(value));
            writer.Write(Encoding.UTF8.GetBytes(value));
        }

        public static string GetString(this BinaryReader reader)
        {
            var length = GetNumber1(reader);
            return Encoding.UTF8.GetString(reader.ReadBytes(length));
        }

        public static void PutLongString(this BinaryWriter writer, string value)
        {
            PutNumber4(writer, Encoding.UTF8.GetByteCount(value));
            writer.Write(Encoding.UTF8.GetBytes(value));
        }

        public static string GetLongString(this BinaryReader reader)
        {
            var length = GetNumber4(reader);
            return Encoding.UTF8.GetString(reader.ReadBytes(length));
        }
    }
}
