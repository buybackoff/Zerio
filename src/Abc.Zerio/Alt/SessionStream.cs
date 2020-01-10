using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace Abc.Zerio.Alt
{
    public class SessionStream : Stream
    {
        private readonly Session _session;

        internal SessionStream(Session session)
        {
            _session = session;
        }

        public override void Flush()
        {
            // noop
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return Read(buffer.AsSpan(offset, count));
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new System.NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new System.NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Write(buffer.AsSpan(offset, count));
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;

        public override bool CanTimeout => true;
        public override long Length { get; }
        public override long Position { get; set; }

        public override int Read(Span<byte> buffer)
        {
            return _session.StreamReceive(buffer);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(), cancellationToken).AsTask();
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
        {
            var result = Read(buffer.Span);
            return new ValueTask<int>(result);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            _session.StreamSend(buffer);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return WriteAsync(buffer.AsMemory(), cancellationToken).AsTask();
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
        {
            Write(buffer.Span);
            return new ValueTask();
        }
    }
}
