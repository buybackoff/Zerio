using System;
using System.Net;
using System.Text;
using System.Threading;
using Abc.Zerio.Core;
using Abc.Zerio.Interop;
using SocketFlags = Abc.Zerio.Interop.SocketFlags;
using SocketType = Abc.Zerio.Interop.SocketType;

namespace Abc.Zerio
{
    public class ZerioClient : IFeedClient
    {
        private readonly IPEndPoint _serverEndpoint;
        private readonly CompletionQueues _completionQueues;
        private readonly ISessionManager _sessionManager;
        private readonly ZerioConfiguration _configuration;
        private readonly Session _session;

        private readonly RequestProcessingEngine _requestProcessingEngine;
        private readonly ReceiveCompletionProcessor _receiveCompletionProcessor;
        private readonly AutoResetEvent _handshakeSignal = new AutoResetEvent(false);

        public  bool IsConnected { get; private set; }
        
        public event Action Connected;
        public event Action Disconnected;
        public event ClientMessageReceivedDelegate MessageReceived;

        public ZerioClient(IPEndPoint serverEndpoint)
        {
            _serverEndpoint = serverEndpoint;

            WinSock.EnsureIsInitialized();

            _configuration = CreateConfiguration();
            _completionQueues = CreateCompletionQueues();
            _sessionManager = CreateSessionManager();

            _requestProcessingEngine = CreateRequestProcessingEngine();
            _receiveCompletionProcessor = CreateReceiveCompletionProcessor();

            _session = _sessionManager.Acquire();
            _session.HandshakeReceived += OnHandshakeReceived;
            _session.Closed += OnSessionClosed;
        }

        private void OnHandshakeReceived(string peerId)
        {
            _handshakeSignal.Set();
        }

        private ISessionManager CreateSessionManager()
        {
            var sessionManager = new SessionManager(_configuration, _completionQueues);
            sessionManager.MessageReceived += (peerId, message) => MessageReceived?.Invoke(message);
            return sessionManager;
        }

        private CompletionQueues CreateCompletionQueues()
        {
            return new CompletionQueues(_configuration);
        }

        private ReceiveCompletionProcessor CreateReceiveCompletionProcessor()
        {
            var receiver = new ReceiveCompletionProcessor(_configuration, _completionQueues.ReceivingQueue, _sessionManager, _requestProcessingEngine);
            return receiver;
        }

        private static ZerioConfiguration CreateConfiguration()
        {
            var zerioConfiguration = ZerioConfiguration.CreateDefault();
            zerioConfiguration.SessionCount = 1;
            return zerioConfiguration;
        }

        private RequestProcessingEngine CreateRequestProcessingEngine()
        {
            return new RequestProcessingEngine(_configuration, _completionQueues.SendingQueue, _sessionManager);
        }

        public void Send(ReadOnlySpan<byte> message)
        {
            _requestProcessingEngine.RequestSend(_session.Id, message);
        }

        public void Start(string peerId)
        {
            if (IsConnected)
                throw new InvalidOperationException("Already started");

            _receiveCompletionProcessor.Start();
            _requestProcessingEngine.Start();

            var socket = CreateSocket();

            _session.Open(socket);

            Connect(socket, _serverEndpoint);

            _session.InitiateReceiving(_requestProcessingEngine);
            
            Handshake(peerId);

            IsConnected = true;
            
            Connected?.Invoke();
        }

        private void Handshake(string peerId)
        { 
            var peerIdBytes = Encoding.ASCII.GetBytes(peerId);
            Send(peerIdBytes.AsSpan());
            _handshakeSignal.WaitOne();
        }

        private static unsafe void Connect(IntPtr socket, IPEndPoint ipEndPoint)
        {
            var endPointAddressBytes = ipEndPoint.Address.GetAddressBytes();
            var inAddress = new InAddr(endPointAddressBytes);

            var sa = new SockaddrIn
            {
                sin_family = AddressFamilies.AF_INET,
                sin_port = WinSock.htons((ushort)ipEndPoint.Port),
                sin_addr = inAddress
            };

            var errorCode = WinSock.connect(socket, ref sa, sizeof(SockaddrIn));
            if (errorCode == WinSock.Consts.SOCKET_ERROR)
                WinSock.ThrowLastWsaError();
        }

        private static unsafe IntPtr CreateSocket()
        {
            var socketFlags = SocketFlags.WSA_FLAG_REGISTERED_IO | SocketFlags.WSA_FLAG_OVERLAPPED;
            var connectionSocket = WinSock.WSASocket(AddressFamilies.AF_INET, SocketType.SOCK_STREAM, Protocol.IPPROTO_TCP, IntPtr.Zero, 0, socketFlags);
            if (connectionSocket == (IntPtr)WinSock.Consts.INVALID_SOCKET)
            {
                WinSock.ThrowLastWsaError();
                return IntPtr.Zero;
            }

            var tcpNoDelay = -1;
            WinSock.setsockopt(connectionSocket, WinSock.Consts.IPPROTO_TCP, WinSock.Consts.TCP_NODELAY, (char*)&tcpNoDelay, sizeof(int));

            var reuseAddr = 1;
            WinSock.setsockopt(connectionSocket, WinSock.Consts.SOL_SOCKET, WinSock.Consts.SO_REUSEADDR, (char*)&reuseAddr, sizeof(int));

            return connectionSocket;
        }

        private void OnSessionClosed(Session session)
        {
            IsConnected = false;
            Disconnected?.Invoke();
        }

        public void Stop()
        {
            _requestProcessingEngine.Stop();
            _receiveCompletionProcessor.Stop();
        }

        public void Dispose()
        {
            Stop();

            _completionQueues?.Dispose();
            _requestProcessingEngine?.Dispose();
            _sessionManager?.Dispose();
        }
    }
}
