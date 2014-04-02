using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace ConsoleApplication2
{
    /// <summary>
    /// Class for handling reading compressed frames of JSON content from a blob stream. Based on producer and consumer
    /// pattern, allowing the frame reading to be threaded.
    /// </summary>
    /// <typeparam name="T">The type of entities that will be returned in the GetNext method.</typeparam>
    public sealed class BlobContentReader<T> : IDisposable, IEnumerable<IList<T>>
    {
        #region Private members

        private Task _producerTask;
        private BlockingCollection<byte[]> _frameQueue;
        private CancellationTokenSource _cancel;
        private ManualResetEvent _headerReady;
        private volatile int _frameCount;
        private volatile bool _cancelled;
        private bool _disposed;

        #endregion

        #region Private methods

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">True if managed resources should be cleaned up.</param>
        private void Dispose(bool disposing)
        {
            if (!disposing || _disposed) return;

            try
            {
                if ((_cancel != null) && (_producerTask != null))
                {
                    _cancel.Cancel();

                    try
                    {
                        _producerTask.Wait();
                    }
                    catch (Exception)
                    {
                        _cancelled = true;
                    }

                    _producerTask.Dispose();
                    _producerTask = null;

                    _cancel.Dispose();
                    _cancel = null;
                }

                if (_headerReady != null)
                {
                    _headerReady.Dispose();
                    _headerReady = null;
                }

                if (_frameQueue != null)
                {
                    _frameQueue.Dispose();
                    _frameQueue = null;
                }
            }
            finally
            {
                _disposed = true;
            }
        }

        /// <summary>
        /// Threaded frame producer that handles all blob stream interaction.
        /// </summary>
        /// <param name="stream">The stream returned as a result of the async open.</param>
        private void Producer(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");

            using (stream)
            {
                try
                {
                    var header = new byte[sizeof (int)*2];
                    var frameSize = new byte[sizeof (int)];

                    ReadStream(stream, header, 0, header.Length);

                    _frameCount = BitConverter.ToInt32(header, 0);

                    _headerReady.Set();

                    for (var i = 0; i < _frameCount; i++)
                    {
                        ReadStream(stream, frameSize, 0, frameSize.Length);

                        var size = BitConverter.ToInt32(frameSize, 0);
                        var frame = new byte[size];

                        ReadStream(stream, frame, 0, size);
                       
                        if (!_frameQueue.TryAdd(frame, Timeout.Infinite, _cancel.Token)) break;
                    }
                }
                catch (OperationCanceledException)
                {
                    _cancelled = true;
                }
                finally
                {
                    _frameQueue.CompleteAdding();
                    _headerReady.Set();
                }
            }
        }

        /// <summary>
        /// Reads data from the blob stream and stores it in the specified buffer.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="buffer">The buffer to place data into.</param>
        /// <param name="offset">The offset in the byte array to start placing data.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        private void ReadStream(Stream stream, byte[] buffer, int offset, int size)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (buffer == null) throw new ArgumentNullException("buffer");
            if (offset < 0) throw new ArgumentOutOfRangeException("offset");
            if (size < 0) throw new ArgumentOutOfRangeException("size");
            if (_cancel.IsCancellationRequested) throw new OperationCanceledException();

            var read = stream.Read(buffer, offset, size);

            while (read < size)
            {
                read += stream.Read(buffer, offset + read, size - read);
            }
        }

        #endregion

        #region Constructor and destructor

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="blobReference">The blob reference that content will be obtained from</param>
        /// <param name="boundedCapacity">The bounded size of the collection. This is used to throttle the producer.</param>
        public BlobContentReader(ICloudBlob blobReference, int boundedCapacity = 128)
        {
            if (blobReference == null) throw new ArgumentNullException("blobReference");

            _frameQueue = new BlockingCollection<byte[]>(boundedCapacity);
            _headerReady = new ManualResetEvent(false);
            _cancel = new CancellationTokenSource();

            _producerTask = blobReference.OpenReadAsync(_cancel.Token).ContinueWith(t => Producer(t.Result));
        }

        /// <summary>
        /// Destructor.
        /// </summary>
        ~BlobContentReader()
        {
            Dispose(false);
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Returns standard enumerator.
        /// </summary>
        /// <returns>Returns standard enumerator.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Returns generic based enumerator.
        /// </summary>
        /// <returns> Returns generic based enumerator.</returns>
        public IEnumerator<IList<T>> GetEnumerator()
        {
            return _frameQueue.GetConsumingEnumerable(_cancel.Token).Select(frame => (IList<T>)JsonConvert.DeserializeObject(Encoding.ASCII.GetString(Zip.Decompress(frame)), typeof (IList<T>))).GetEnumerator();
        }

        #endregion

        #region Public properties

        /// <summary>
        /// Returns the number of frames that the stream contains.
        /// </summary>
        public int FrameCount
        {
            get
            {
                return (_headerReady.WaitOne() ? _frameCount : 0);
            }
        }

        /// <summary>
        /// Returns true if all the frames have been consumed, otherwise false.
        /// </summary>
        public bool Eof
        {
            get
            {
                return (_frameQueue.IsAddingCompleted && (_frameQueue.Count == 0));
            }
        }

        /// <summary>
        /// Returns true if the streaming was cancelled, otherwise false.
        /// </summary>
        public bool Cancelled
        {
            get
            {
                return _cancelled;
            }
        }

        #endregion
    }
}