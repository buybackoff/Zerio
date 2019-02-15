using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Abc.Zerio.Core;
using Abc.Zerio.Dispatch;
using Abc.Zerio.Framing;
using Abc.Zerio.Serialization;
using CommandLine;
using HdrHistogram;

namespace Abc.Zerio.Ping
{
    static class Program
    {
        const double _microsecDivisor = TimeSpan.TicksPerMillisecond / 1000.0;
        private const int _warmupIterations = 100;
        private const int _sendPort = 12345;

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                  .WithParsed(a =>
                  {
                      var serializationRegistries = new SerializationRegistries();
                      serializationRegistries.ForBoth(r => r.Register<Ping, PingSerializer>());
                      serializationRegistries.ForBoth(r => r.Register<Pong, PongSerializer>());

                      if (a.IsServer)
                          RunPong(a, serializationRegistries);
                      else
                          RunPing(a, serializationRegistries);
                  });
        }

        private static void RunPing(Options options, SerializationRegistries serializationRegistries)
        {
            var histogram = new LongHistogram(TimeSpan.FromSeconds(10).Ticks, 4);

            var rand = new Random();
            var writeBuffer = Enumerable.Range(0, options.Length).Select(x => (byte)rand.Next(0, 256)).ToArray();

            var clientConfiguration = new ClientConfiguration();
            SetupConfiguration(clientConfiguration);

            var client = new RioClient(clientConfiguration, new SerializationEngine(serializationRegistries.Client));
            var ipAddresses = Dns.GetHostAddresses(options.HostName);
            if (ipAddresses.Length < 1)
            {
                Console.WriteLine($"Could not find IP address for host '{options.HostName}'");
                return;
            }
            var serverEndPoint = new IPEndPoint(ipAddresses[0], _sendPort);

            var finished = new ManualResetEvent(false);
            var pongsReceived = 0;
            client.Subscribe<Pong>(pong =>
            {
                var ticks = Stopwatch.GetTimestamp() - pong.PingTimestamp;
                if (pongsReceived > _warmupIterations)
                    histogram.RecordValue(ticks);

                if (!options.IsQuiet)
                    Console.WriteLine($"Ping {serverEndPoint.ToString()} in {ticks / _microsecDivisor}µs");

                pongsReceived++;
                if (pongsReceived == _warmupIterations + options.Count)
                    finished.Set();
            });

            client.Connect(serverEndPoint);
            Console.WriteLine("client configuration: ");
            Console.WriteLine(clientConfiguration);
            Console.WriteLine($"Connected to server {options.HostName}, pinging with buffer of size {writeBuffer.Length}");

            // warmup
            Console.WriteLine("Warming up...");
            for (var i = 0; i < _warmupIterations; i++)
            {
                client.Send(new Ping { Timestamp = Stopwatch.GetTimestamp(), Payload = writeBuffer });
                Wait(0);
            }

            // real run
            Console.WriteLine("Warmup complete, starting real run...");
            for (var i = 0; i < options.Count; i++)
            {
                client.Send(new Ping { Timestamp = Stopwatch.GetTimestamp(), Payload = writeBuffer });
                Wait(options.IntervalInMilliseconds);
            }

            Console.WriteLine("All pings sent, waiting for all replies...");

            finished.WaitOne(TimeSpan.FromMinutes(1));
            Console.WriteLine("All replies received");

            client.Dispose();

            histogram.OutputPercentileDistribution(Console.Out, percentileTicksPerHalfDistance: 2, outputValueUnitScalingRatio: _microsecDivisor);
        }

        private static void RunPong(Options options, SerializationRegistries serializationRegistries)
        {
            var serverConfiguration = new ServerConfiguration(_sendPort);
            SetupConfiguration(serverConfiguration);
            var server = new RioServer(serverConfiguration, new SerializationEngine(serializationRegistries.Server));
            server.ClientConnected += c => Console.WriteLine("Client connected");
            server.ClientDisconnected += c => Console.WriteLine("Client disconnected");

            var token = server.Subscribe<Ping>((clientId, ping) =>
            {
                Console.WriteLine("plop");
                // ReSharper disable once AccessToDisposedClosure
                server.Send(clientId, new Pong { PingTimestamp = ping.Timestamp, Payload = ping.Payload });
            });
            server.Start();
            Console.WriteLine("Server configuration: ");
            Console.WriteLine(serverConfiguration);
            Console.WriteLine($"Pong server running on port {_sendPort}, press any key to stop server...");

            Console.ReadKey(true);
            token.Dispose();
            server.Dispose();
        }

        private static void SetupConfiguration(RioConfiguration configuration)
        {
            var bufferLength = 4096;
            var bufferCount = 2000;
            configuration.ReceivingBufferLength = bufferLength;
            configuration.ReceivingBufferCount = bufferCount;
            configuration.SendingBufferLength = bufferLength;
            configuration.SendingBufferCount = bufferCount;
        }

        private static void Wait(int intervalInMilliseconds)
        {
            if (intervalInMilliseconds > 0)
                Thread.Sleep(intervalInMilliseconds);
            else
                Thread.SpinWait(1 << 8);
        }

        public class Options
        {
            [Option('s', "server", Default = false)]
            public bool IsServer { get; set; }

            [Option('h', "hostname", Default = "localhost")]
            public string HostName { get; set; }

            [Option('c', "count", Default = 4)]
            public int Count { get; set; }

            [Option('q', "quiet", Default = false)]
            public bool IsQuiet { get; set; }

            [Option('i', "interval", Default = 0)]
            public int IntervalInMilliseconds { get; set; }

            [Option('l', "length", Default = 512)]
            public int Length { get; set; }
        }

        private class SerializationRegistries
        {
            public SerializationRegistry Client { get; } = new SerializationRegistry(Encoding.ASCII);
            public SerializationRegistry Server { get; } = new SerializationRegistry(Encoding.ASCII);

            public void ForBoth(Action<SerializationRegistry> action)
            {
                action.Invoke(Client);
                action.Invoke(Server);
            }
        }

        public class Ping
        {
            public long Timestamp { get; set; }
            public byte[] Payload { get; set; }
        }

        public class Pong
        {
            public long PingTimestamp { get; set; }
            public byte[] Payload { get; set; }
        }

        public class PingSerializer : BinaryMessageSerializer<Ping>
        {
            public override void Serialize(Ping message, UnsafeBinaryWriter binaryWriter)
            {
                binaryWriter.Write(message.Timestamp);
                binaryWriter.Write(message.Payload.Length);
                binaryWriter.Write(message.Payload);
            }

            public override void Deserialize(Ping message, UnsafeBinaryReader binaryReader)
            {
                message.Timestamp = binaryReader.ReadInt64();
                var length = binaryReader.ReadInt32();
                message.Payload = new byte[length];
                binaryReader.Read(message.Payload, 0, length);
            }
        }

        public class PongSerializer : BinaryMessageSerializer<Pong>
        {
            public override void Serialize(Pong message, UnsafeBinaryWriter binaryWriter)
            {
                binaryWriter.Write(message.PingTimestamp);
                binaryWriter.Write(message.Payload.Length);
                binaryWriter.Write(message.Payload);
            }

            public override void Deserialize(Pong message, UnsafeBinaryReader binaryReader)
            {
                message.PingTimestamp = binaryReader.ReadInt64();
                var length = binaryReader.ReadInt32();
                message.Payload = new byte[length];
                binaryReader.Read(message.Payload, 0, length);
            }
        }
    }
}
