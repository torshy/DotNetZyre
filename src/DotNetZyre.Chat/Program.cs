using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;

using NetMQ;

namespace DotNetZyre.Chat
{
    class Program
    {
        static void Main(string[] args)
        {
#if DEBUG
            Trace.Listeners.Add(new ConsoleTraceListener());
#endif

            Console.WriteLine("********* ZYRE CHAT *********");
            Console.Write("* Enter nickname >> ");

            var name = Console.ReadLine();
            if (string.IsNullOrEmpty(name))
            {
                name = Process.GetCurrentProcess().Id.ToString(CultureInfo.InvariantCulture);
                Console.SetCursorPosition("* Enter nickname >> ".Length, Console.CursorTop - 1);
                Console.WriteLine(name);
            }

            var address = "*";
            var allNetworkInterfaces = NetworkInterface.GetAllNetworkInterfaces();
            if (allNetworkInterfaces.Length > 1)
            {
                Console.WriteLine("* Select interface:");

                for (int index = 0; index < allNetworkInterfaces.Length; index++)
                {
                    var networkInterface = allNetworkInterfaces[index];
                    Console.WriteLine("* {0}: {1}", (index + 1), networkInterface.Name);
                }

                Console.Write("> ");
                var result = Console.ReadLine();

                int interfaceIndex;
                if (int.TryParse(result, out interfaceIndex))
                {
                    interfaceIndex--;
                    var nic = NetworkInterface.GetAllNetworkInterfaces()[interfaceIndex];
                    var unicast = nic.GetIPProperties().UnicastAddresses;
                    var unicastAddress =
                        unicast.FirstOrDefault(u => u.Address.AddressFamily == AddressFamily.InterNetwork);
                    if (unicastAddress != null)
                    {
                        address = unicastAddress.Address.ToString();
                    }

                    Console.WriteLine("You choose {0} at {1}", nic.Name, address);
                }
            }

            Console.WriteLine("*****************************");

            using (var context = NetMQContext.Create())
            {
                var actor = NetMQActor.Create(context, new Chat(context, name));
                actor.SendMore(Chat.InterfaceCommand).Send(address);
                actor.Send(Chat.StartCommand);

                var input = string.Empty;
                while (!string.Equals(input, "EXIT", StringComparison.InvariantCultureIgnoreCase))
                {
                    input = Console.ReadLine();
                    if (!string.IsNullOrWhiteSpace(input))
                    {
                        actor.SendMore(Chat.ShoutCommand).Send(input);
                    }
                }
            }
        }
    }
}
