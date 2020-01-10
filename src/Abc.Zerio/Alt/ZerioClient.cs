using System;
using System.Net;
using System.Text;
using System.Threading;
using Abc.Zerio.Alt.Buffers;
using Abc.Zerio.Core;
using Abc.Zerio.Interop;
using SocketFlags = Abc.Zerio.Interop.SocketFlags;
using SocketType = Abc.Zerio.Interop.SocketType;

namespace Abc.Zerio.Alt
{
    public class ZerioClient : IFeedClient
    {
        private readonly IPEndPoint _serverEndpoint;
        private Session _session;
        private IntPtr _socket;
        private int _started;

        public bool IsConnected { get; private set; }

        public event Action Connected;
        public event Action Disconnected;
        public event ClientMessageReceivedDelegate MessageReceived;

        private RioBufferPool _pool;
        private CancellationTokenSource _cts;
        private Poller _poller;

        public ZerioClient(IPEndPoint serverEndpoint)
        {
            _serverEndpoint = serverEndpoint;

            WinSock.EnsureIsInitialized();

            _cts = new CancellationTokenSource();
            _pool = new RioBufferPool(ct: _cts.Token);
            _poller = new Poller("client_poller", _cts.Token);
        }

        public void Send(ReadOnlySpan<byte> message)
        {
            _session.Send(message);
        }

        private void CheckOnlyStartedOnce()
        {
            if (Interlocked.Exchange(ref _started, 1) != 0)
                throw new InvalidOperationException($"{nameof(ZerioClient)} must only be started once.");
        }

        public void Start(string peerId)
        {
            if (IsConnected)
                throw new InvalidOperationException("Already started");

            CheckOnlyStartedOnce();

            _socket = CreateSocket();
            Connect(_socket, _serverEndpoint);

            _session = new Session(false, _socket, _pool, _poller, (_, bytes) => { MessageReceived?.Invoke(bytes); }, OnSessionClosed);
            var peerIdBytes = Encoding.ASCII.GetBytes(peerId);
            _session.StreamSend(peerIdBytes.AsSpan());
            _session.HandshakeSignal.WaitOne();

            IsConnected = true;
            Connected?.Invoke();
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

            var fastLoopback = 1;
            const int SIO_LOOPBACK_FAST_PATH = -1744830448;
            WinSock.WSAIoctlGeneral(connectionSocket,
                (IntPtr) SIO_LOOPBACK_FAST_PATH,
                &fastLoopback,
                4,
                null,
                0,
                out _,
                IntPtr.Zero,
                IntPtr.Zero);

            return connectionSocket;
        }

        private void OnSessionClosed(Session session)
        {
            IsConnected = false;
            Disconnected?.Invoke();
        }

        public void Stop()
        {
            if (!IsConnected)
                throw new InvalidOperationException("Already stopped");
            _cts.Cancel();
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                _session.Dispose();
                _session = null;
                _pool.Dispose();
                _poller.Dispose();
            }
            else
            {
                if (_socket != IntPtr.Zero)
                    WinSock.closesocket(_socket);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~ZerioClient()
        {
            Dispose(false);
        }
    }
}
