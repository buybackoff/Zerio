using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Abc.Zerio.Core;

namespace Abc.Zerio.Tcp
{
    public class TcpFeedServer : IFeedServer
    {
        private readonly Dictionary<string, ClientSession> _clients = new Dictionary<string, ClientSession>();
        private readonly int _port;
        private Socket _listeningSocket;
        
        private volatile bool _isRunning;

        public TcpFeedServer(int port)
        {
            _port = port;
        }

        public bool IsRunning => _isRunning;
        public event Action<string> ClientConnected = delegate { };
        public event Action<string> ClientDisconnected = delegate { };
        public event ServerMessageReceivedDelegate MessageReceived;

        public void Start(string peerId)
        {
            if (_isRunning)
                throw new InvalidOperationException("Already started");

            _isRunning = true;

            _listeningSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                ExclusiveAddressUse = true,
                NoDelay = true
            };

            _listeningSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
            _listeningSocket.Listen(16);

            StartAccepting();
        }

        public void StartAccepting()
        {
            Task.Factory.StartNew(AcceptingLoop, TaskCreationOptions.LongRunning);
        }

        private void AcceptingLoop()
        {
            try
            {
                while (_isRunning)
                {
                    var clientSocket = _listeningSocket.Accept();
                    InitClient(clientSocket);
                }
            }
            catch (ObjectDisposedException)
            {
                // listening socket was disposed
            }
        }
        
        private void InitClient(Socket socket)
        {
            var peerId = Guid.NewGuid().ToString();

            socket.NoDelay = true;

            var client = new ClientSession(socket);

            lock (_clients)
            {
                _clients.Add(peerId, client);
            }

            ClientConnected?.Invoke(peerId);

            client.MessageReceived += message => OnMessageReceived(peerId, message);
            client.Receive();
        }

        private void OnMessageReceived(string peerId, ReadOnlySpan<byte> message)
        {
            MessageReceived?.Invoke(peerId, message);
        }

        public void Send(string peerId, ReadOnlySpan<byte> message)
        {
            if (message.Length == 0)
                return;

            ClientSession client;

            lock (_clients)
            {
                if (!_clients.TryGetValue(peerId, out client))
                    throw new InvalidOperationException($"Not connected to {peerId}");
            }

            client.Send(message);
        }

        public void Stop()
        {
            if (!_isRunning)
                return;

            _isRunning = false;

            _listeningSocket?.Dispose();
            _listeningSocket = null;

            lock (_clients)
            {
                foreach (var (_, client) in _clients)
                    client.Dispose();

                _clients.Clear();
            }
        }

        private class ClientSession : IDisposable
        {
            private readonly Socket _socket;
            private readonly TcpFrameReceiver _receiver;
            private readonly TcpFrameSender _sender;

            public event ClientMessageReceivedDelegate MessageReceived;

            public ClientSession(Socket socket)
            {
                _socket = socket;

                _sender = new TcpFrameSender(socket);
                _receiver = new TcpFrameReceiver(socket);
                _receiver.MessageReceived += message => MessageReceived?.Invoke(message);
            }

            public void Dispose()
            {
                _socket.Dispose();
            }

            public void Receive() => _receiver.StartReceive();

            public void Send(ReadOnlySpan<byte> message) => _sender.Send(message);
        }

        public void Dispose()
        {
            _listeningSocket?.Dispose();
        }
    }
}
