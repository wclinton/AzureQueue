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
    public sealed class BlobContentReader<T> : IDisposable, IEnumerable<T>
    {
        #region Private members

        private Task producerTask;
        private BlockingCollection<byte[]> frameQueue;
   //     private CancellationTokenSource cancel;
        private ManualResetEvent headerReady;
        private volatile int frameCount;
        private volatile bool cancelled;
        private bool disposed;


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="blobReference">The blob reference that content will be obtained from</param>
        /// <param name="boundedCapacity">The bounded size of the collection. This is used to throttle the producer.</param>
        public BlobContentReader(ICloudBlob blobReference, int boundedCapacity = 128)
            : this(blobReference.OpenRead(), boundedCapacity)
        {
            if (blobReference == null) throw new ArgumentNullException("blobReference");         
        }


        public BlobContentReader(Stream stream, int boundedCapacity = 128)
        {

            frameQueue = new BlockingCollection<byte[]>(boundedCapacity);
            headerReady = new ManualResetEvent(false);
         //   cancel = new CancellationTokenSource();

            

            producerTask = new Task(() => Producer(stream));
            producerTask.RunSynchronously();
        }



        #endregion

        #region Private methods

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">True if managed resources should be cleaned up.</param>
        private void Dispose(bool disposing)
        {
            if (!disposing || disposed) return;

            try
            {
               if /* ((cancel != null) &&*/ (producerTask != null)
                {
                    //cancel.Cancel();

                    try
                    {
                        producerTask.Wait();
                    }
                    catch (Exception)
                    {
                        cancelled = true;
                    }

                    producerTask.Dispose();
                    producerTask = null;
//
//                    cancel.Dispose();
//                    cancel = null;
                }

                if (headerReady != null)
                {
                    headerReady.Dispose();
                    headerReady = null;
                }

                if (frameQueue != null)
                {
                    frameQueue.Dispose();
                    frameQueue = null;
                }
            }
            finally
            {
                disposed = true;
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
                    var header = new byte[sizeof(int) * 2];
                    var frameSize = new byte[sizeof(int)];

                    ReadStream(stream, header, 0, header.Length);

                    frameCount = BitConverter.ToInt32(header, 0);

                    headerReady.Set();

                    for (var i = 0; i < frameCount; i++)
                    {
                        ReadStream(stream, frameSize, 0, frameSize.Length);

                        var size = BitConverter.ToInt32(frameSize, 0);
                        var frame = new byte[size];

                        ReadStream(stream, frame, 0, size);

                        if (!frameQueue.TryAdd(frame, Timeout.Infinite)) break;
                    }
                }
                catch (OperationCanceledException)
                {
                    cancelled = true;
                }
                finally
                {
                    frameQueue.CompleteAdding();
                    headerReady.Set();
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
      //      if (cancel.IsCancellationRequested) throw new OperationCanceledException();

            var read = stream.Read(buffer, offset, size);

            while (read < size)
            {
                read += stream.Read(buffer, offset + read, size - read);
            }
        }

        #endregion

        #region Constructor and destructor



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
        public IEnumerator<T> GetEnumerator()
        {
            var list = frameQueue.GetConsumingEnumerable().Select(frame => (IList<T>)
                JsonConvert.DeserializeObject(Encoding.ASCII.GetString(Zip.Decompress(frame)), typeof(IList<T>)));


            IList<T> outList = new List<T>();
            foreach (IEnumerable<T> item in list)
            {
                foreach (var i in item)
                {
                    outList.Add(i);    
                }                
            }
            return outList.GetEnumerator();
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
                return (headerReady.WaitOne() ? frameCount : 0);
            }
        }

        /// <summary>
        /// Returns true if all the frames have been consumed, otherwise false.
        /// </summary>
        public bool Eof
        {
            get
            {
                return (frameQueue.IsAddingCompleted && (frameQueue.Count == 0));
            }
        }

        /// <summary>
        /// Returns true if the streaming was cancelled, otherwise false.
        /// </summary>
        public bool Cancelled
        {
            get
            {
                return cancelled;
            }
        }

        #endregion
    }
}