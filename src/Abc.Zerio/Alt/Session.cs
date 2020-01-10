using Abc.Zerio.Alt.Buffers;
using Abc.Zerio.Interop;
using System;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Abc.Zerio.Alt
{
    internal delegate void SessionMessageReceivedDelegate(string peerId, ReadOnlySpan<byte> message);

    internal unsafe class Session : SessionSCQLockRhsPad, IDisposable
    {
        private readonly BoundedLocalPool<RioSegment> _localSendPool;
        private readonly RegisteredBuffer _localSendBuffer;
        private readonly RegisteredBuffer _localReceiveBuffer;

        // private readonly RioBufferPool _globalPool;

        private readonly Poller _poller;
        private volatile bool _shouldPollReceive;

        private CancellationToken _ct;

        private readonly IntPtr _scq;
        private readonly IntPtr _rcq;
        private const int _sendPollCount = 64;
        private const int _receivePollCount = 64;

        private readonly IntPtr _rq; // TODO there is no Close method on RQ, review if need to do something else with it.
        private readonly RioSendReceive _sendReceive;

        private readonly SemaphoreSlimRhsPad _sendSemaphore;

        public readonly AutoResetEvent HandshakeSignal = new AutoResetEvent(false);
        public string PeerId { get; private set; }

        private volatile bool _isWaitingForHandshake = true;
        private readonly bool _isServer;
        private readonly IntPtr _socketHandle;
        private readonly SessionMessageReceivedDelegate _messageReceived;
        private readonly Action<Session> _closed;
        private readonly byte* _resultsPtr;
        private readonly RIO_RESULT* _sendResults;
        private readonly RIO_RESULT* _receiveResults;
        private int _receiveResultsCount;
        private int _receiveResultsProcessed;

        private byte[] _sendBuffer = new byte[64 * 1024];
        private byte[] _receiveBuffer = new byte[64 * 1024];
        private byte[] _localRemainingRead;
        private int _localRemainingLength;
        private int _localRemainingOffset;

        private int _receivedBytes;
        private WebSocket _ws;

        public Session(bool isServer, IntPtr socketHandle, RioBufferPool pool, Poller poller, SessionMessageReceivedDelegate messageReceived, Action<Session> closed)
        {
            _isServer = isServer;
            _socketHandle = socketHandle;

            var options = pool.Options;
            _ct = pool.CancellationToken;
            // _globalPool = pool;

            _localSendPool = new BoundedLocalPool<RioSegment>(options.SendSegmentCount);
            _localSendBuffer = new RegisteredBuffer(options.SendSegmentCount, options.SendSegmentSize);
            _localReceiveBuffer = new RegisteredBuffer(options.ReceiveSegmentCount, options.ReceiveSegmentSize);
            _localRemainingRead = new byte[_localReceiveBuffer.SegmentLength];
            _localRemainingLength = 0;
            _localRemainingOffset = 0;

            for (int i = 0; i < options.SendSegmentCount; i++)
            {
                _localSendPool.Return(_localSendBuffer[i]);
            }

            _messageReceived = messageReceived;
            _closed = closed;

            _sendSemaphore = new SemaphoreSlimRhsPad(_localSendPool.Count, _localSendPool.Count);

            _resultsPtr = (byte*)Marshal.AllocHGlobal(Unsafe.SizeOf<RIO_RESULT>() * (_sendPollCount + _receivePollCount) + Padding.SafeCacheLine * 3);
            //  128 ... send_results ... 128 ... receive_results ... 128
            _sendResults = (RIO_RESULT*)(_resultsPtr + Padding.SafeCacheLine);

            _receiveResults = (RIO_RESULT*)((byte*)_sendResults + +(uint)Unsafe.SizeOf<RIO_RESULT>() * _sendPollCount + Padding.SafeCacheLine);
            _receiveResultsCount = 0;
            _receiveResultsProcessed = 0;

            _scq = WinSock.Extensions.CreateCompletionQueue((uint)options.SendSegmentCount);
            _rcq = WinSock.Extensions.CreateCompletionQueue((uint)options.ReceiveSegmentCount);

            _rq = WinSock.Extensions.CreateRequestQueue(socketHandle,
                                                        (uint)options.ReceiveSegmentCount,
                                                        1,
                                                        (uint)options.SendSegmentCount,
                                                        1,
                                                        _rcq,
                                                        _scq,
                                                        0);

            _sendReceive = new RioSendReceive(_rq);

            for (int i = 0; i < options.ReceiveSegmentCount; i++)
            {
                var segment = _localReceiveBuffer[i];
                _sendReceive.Receive(segment);
            }

            _poller = poller;
            _shouldPollReceive = true;

            var stream = new SessionStream(this);
            _ws = WebSocket.CreateFromStream(stream, isServer, "zerio", TimeSpan.FromSeconds(30));
            // this line at the very end after session is ready
            _poller.AddSession(this);
        }

        private void OnMessageReceived(ReadOnlySpan<byte> message)
        {
            _messageReceived?.Invoke(PeerId, message);
        }

        public int StreamReceive(Span<byte> buffer)
        {
            var length = buffer.Length;
            var consumed = 0;

            if (_localRemainingLength > 0)
            {
                var localSpan = _localRemainingRead.AsSpan(_localRemainingOffset, _localRemainingLength - _localRemainingOffset);

                if (localSpan.Length >= buffer.Length)
                {
                    localSpan.Slice(buffer.Length).CopyTo(buffer);
                    _localRemainingOffset += buffer.Length;
                    if (_localRemainingOffset >= _localRemainingLength)
                    {
                        _localRemainingOffset = _localRemainingLength = 0;
                    }

                    return buffer.Length;
                }

                localSpan.CopyTo(buffer);
                consumed += localSpan.Length;
                _localRemainingOffset = _localRemainingLength = 0;
            }

            if (_receiveResultsProcessed >= _receiveResultsCount)
            {
                throw new InvalidOperationException("Should not call Receive until results are available");
            }

            // TODO still assumes that message size < segment size

            while (consumed < length)
            {
                var bytesToConsumeFromSegment = Math.Min(buffer.Length - consumed, _localReceiveBuffer.SegmentLength);

                var result = _receiveResults[_receiveResultsProcessed];
                var id = new BufferSegmentId(result.RequestCorrelation);
                var segment = _localReceiveBuffer[id.SegmentId];

                var copyLength = Math.Min((int)result.BytesTransferred, bytesToConsumeFromSegment);
                var span = segment.Span.Slice(0, copyLength);
                span.CopyTo(buffer.Slice(consumed));
                consumed += span.Length;

                var remainingTransferredBytes = (int)(result.BytesTransferred - copyLength);
                if (remainingTransferredBytes > 0)
                {
                    _localRemainingLength = remainingTransferredBytes;
                    _localRemainingOffset = 0;
                    segment.Span.Slice(copyLength, remainingTransferredBytes).CopyTo(_localRemainingRead);
                    Debug.Assert(consumed == length);
                }

                _sendReceive.Receive(segment);
                _receiveResultsProcessed++;

                if (_receiveResultsProcessed == _receiveResultsCount)
                {
                    _receiveResultsProcessed = _receiveResultsCount = 0;
                    if (!_isWaitingForHandshake)
                    {
                        var availableReceives = DoReceive();
                        if (availableReceives == 0)
                        {
                            return consumed;
                        }
                        else
                        {
                            Console.WriteLine("xxx");
                        }
                    }
                    else
                    {
                        return consumed;
                    }
                }
            }

            return consumed;

            //for (int i = _receiveResultsProcessed; i < _receiveResultsCount; i++)
            //{
            //    var result = _receiveResults[i];
            //    var id = new BufferSegmentId(result.RequestCorrelation);
            //    var segment = _localReceiveBuffer[id.SegmentId];
            //    var span = segment.Span.Slice(0, (int)result.BytesTransferred);
            //    OnSegmentReceived(span);
            //    _sendReceive.Receive(segment);
            //}

            //_receiveResultsCount = 0;
            //_receiveResultsProcessed = 0;
        }

        public void Send(ReadOnlySpan<byte> message)
        {
            message.CopyTo(_sendBuffer);
            // implemented as sync
            _ws.SendAsync(_sendBuffer.AsMemory().Slice(0, message.Length), WebSocketMessageType.Binary, true, _ct);
        }

        public void StreamSend(ReadOnlySpan<byte> message)
        {
            var consumed = 0;
            while (consumed < message.Length)
            {
                var segment = Claim();
                var copyLength = Math.Min(message.Length - consumed, segment.Length);
                message.Slice(consumed, copyLength).CopyTo(segment.Span);
                Commit(segment, copyLength);
                consumed += copyLength;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RioSegment Claim()
        {
            bool isPollerThread = Thread.CurrentThread.ManagedThreadId == _poller.ThreadId;
            var count = 0;
            const int spinLimit = 25;
            while (true)
            {
                bool entered = false;
                if (count >= spinLimit && !isPollerThread)
                {
                    _sendSemaphore.Wait(_ct);
                    entered = true;
                }
                else if (count == 0 || !isPollerThread)
                {
                    entered = _sendSemaphore.Wait(0);
                }

                RioSegment segment;
                if (entered)
                {
                    if (!_localSendPool.TryRent(out segment))
                    {
                        ThrowCannotGetSegmentAfterSemaphoreEnter();
                    }

                    return segment;
                }

                if (isPollerThread
                    || 1 == Interlocked.Increment(ref _isPollingSend)) // 0 -> 1
                {
                    RIO_RESULT result = default;
                    var resultCount = WinSock.Extensions.DequeueCompletion(_scq, &result, 1);
                    Volatile.Write(ref _isPollingSend, 0);

                    if (resultCount == WinSock.Consts.RIO_CORRUPT_CQ)
                        WinSock.ThrowLastWsaError();

                    if (resultCount == 1)
                    {
                        var id = new BufferSegmentId(result.RequestCorrelation);
                        segment = _localSendBuffer[id.SegmentId]; //id.PoolId == 0 ? _localSendBuffer[id.SegmentId] : _globalPool[id];
                        return segment;
                    }
                }

                count++;
                Thread.SpinWait(Math.Min(count, spinLimit));
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowCannotGetSegmentAfterSemaphoreEnter()
        {
            throw new InvalidOperationException("_localSendPool.TryRent(out segment)");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Commit(RioSegment segment, int length)
        {
            segment.RioBuf.Length = length;
            _sendReceive.Send(segment);
        }

        /// <summary>
        /// This method is called by <see cref="Poller"/> on its thread.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining
#if NETCOREAPP3_0
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public int Poll()
        {
            int count = 0;

            count += PollReceive();

            // poll send only if there are no receives
            if (count == 0)
                count += PollSend();

            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int PollReceive()
        {
            if (!_shouldPollReceive && !_isWaitingForHandshake)
                return 0;

            var results = DoReceive();

            if (results == 0)
                return 0;

            if (_isWaitingForHandshake)
            {
                _isWaitingForHandshake = false;
                Span<byte> span = stackalloc byte[128];
                var len = StreamReceive(span);
                var message = span.Slice(0, len);
                PeerId = Encoding.ASCII.GetString(message);
                
                HandshakeSignal.Set();
                if(_isServer)
                    StreamSend(message);

                Debug.Assert(_receiveResultsCount == 0);
                Debug.Assert(_receiveResultsProcessed == 0);

                return 1;
            }

            // this is implemented as sync call: SessionStream.ReceiveAsync -> Session.Receive
            var receiveResult = _ws.ReceiveAsync(_receiveBuffer.AsMemory(_receivedBytes, _receiveBuffer.Length - _receivedBytes), _ct).Result;

            if (receiveResult.MessageType == WebSocketMessageType.Close)
                _shouldPollReceive = false;

            _receivedBytes += receiveResult.Count;

            if (receiveResult.EndOfMessage)
            {
                OnMessageReceived(_receiveBuffer.AsSpan(0, _receivedBytes));
                _receivedBytes = 0;
            }

            return receiveResult.Count;

            //var resultCount = DoReceive();

            //for (int i = _receiveResultsProcessed; i < _receiveResultsCount; i++)
            //{
            //    var result = _receiveResults[i];
            //    var id = new BufferSegmentId(result.RequestCorrelation);
            //    var segment = _localReceiveBuffer[id.SegmentId];
            //    var span = segment.Span.Slice(0, (int)result.BytesTransferred);
            //    OnSegmentReceived(span);
            //    _sendReceive.Receive(segment);
            //}

            //_receiveResultsCount = 0;
            //_receiveResultsProcessed = 0;

            //return (int)resultCount;
        }

        /// <summary>
        /// Dequeue receive results up to <see cref="_receivePollCount"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int DoReceive()
        {
            var resultCount = WinSock.Extensions.DequeueCompletion(_rcq,
                                                                   _receiveResults + _receiveResultsCount,
                                                                   _isWaitingForHandshake ? 1 : (uint)(_receivePollCount - _receiveResultsCount));
            if (resultCount == WinSock.Consts.RIO_CORRUPT_CQ)
                WinSock.ThrowLastWsaError();
            _receiveResultsCount += (int)resultCount;
            return _receiveResultsCount - _receiveResultsProcessed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int PollSend()
        {
            if (1 != Interlocked.Increment(ref _isPollingSend))
                return 0;

            var resultCount = WinSock.Extensions.DequeueCompletion(_scq, _sendResults, _sendPollCount);

            Volatile.Write(ref _isPollingSend, 0);

            if (resultCount == WinSock.Consts.RIO_CORRUPT_CQ)
                WinSock.ThrowLastWsaError();

            for (int i = 0; i < resultCount; i++)
            {
                var result = _sendResults[i];
                var id = new BufferSegmentId(result.RequestCorrelation);
                if (id.PoolId == 0)
                {
                    var segment = _localSendBuffer[id.SegmentId];
                    _localSendPool.Return(segment);
                }
                else
                {
                    throw new InvalidOperationException();
                    //var segment = _globalPool[id];
                    //_globalPool.Return(segment);
                }

                // only after adding to the pool
                _sendSemaphore.Release();
            }

            return (int)resultCount;
        }

        //private void OnSegmentReceived(Span<byte> bytes)
        //{
        //    _messageFramer.SubmitBytes(bytes);
        //}

        ~Session()
        {
            Dispose(false);
        }

        private void ReleaseUnmanagedResources()
        {
            Marshal.FreeHGlobal((IntPtr)_resultsPtr);
            WinSock.Extensions.CloseCompletionQueue(_rcq);
            WinSock.Extensions.CloseCompletionQueue(_scq);
            WinSock.closesocket(_socketHandle);
        }

        private void Dispose(bool disposing)
        {
            ReleaseUnmanagedResources();
            if (disposing)
            {
                _poller.RemoveSession(this);
                _closed.Invoke(this);
                // After setting this segments of these buffers won't be returned to the pools
                // and the buffers will be disposed as soon as no segments are used.

                // TODO drain outstanding

                _localReceiveBuffer.IsPooled = false;
                _localSendBuffer.IsPooled = false;
                _sendSemaphore?.Dispose();
                HandshakeSignal?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private class RioSendReceive
        {
            // TODO SpinLock with padding
            private readonly IntPtr _rq;

            public RioSendReceive(IntPtr rq)
            {
                _rq = rq;
            }

            public void Send(RioSegment segment)
            {
                // SpinLock is not better, already tried
                lock (this)
                {
                    if (!WinSock.Extensions.Send(_rq, &segment.RioBuf, 1, RIO_SEND_FLAGS.DONT_NOTIFY, segment.Id.Value))
                        WinSock.ThrowLastWsaError();
                }
            }

            public void Receive(RioSegment segment)
            {
                lock (this)
                {
                    if (!WinSock.Extensions.Receive(_rq, &segment.RioBuf, 1, RIO_RECEIVE_FLAGS.DONT_NOTIFY, segment.Id.Value))
                        WinSock.ThrowLastWsaError();
                }
            }
        }

        /// <summary>
        /// m_currentCount is at offset 32 with 12 bytes after in <see cref="SemaphoreSlim"/>. We cannot pad before, but at least
        /// </summary>
        private class SemaphoreSlimRhsPad : SemaphoreSlim
        {
#pragma warning disable 169
            private Padding _padding;
#pragma warning restore 169

            public SemaphoreSlimRhsPad(int initialCount, int maxCount)
                : base(initialCount, maxCount)
            {
            }
        }
    }

    // ReSharper disable InconsistentNaming

    [StructLayout(LayoutKind.Sequential, Size = SafeCacheLine - 8)]
    internal readonly struct Padding
    {
        public const int SafeCacheLine = 128;
    }

    internal class SessionLhsPad : CriticalFinalizerObject
    {
#pragma warning disable 169
        private Padding _padding;
#pragma warning restore 169
    }

    internal class SessionSCQLock : SessionLhsPad
    {
        protected int _isPollingSend;
#pragma warning disable 169
        private readonly int _padding;
#pragma warning restore 169
    }

    internal class SessionSCQLockRhsPad : SessionSCQLock
    {
#pragma warning disable 169
        private Padding _padding;
#pragma warning restore 169
    }

    // ReSharper restore InconsistentNaming
}
