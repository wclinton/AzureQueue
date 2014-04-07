using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BlobHandler
{
    /// <summary>
    /// Class for handling creation of a multi-frame file to upload to blob storage.
    /// </summary>
    public sealed class BlobContentWriter : IDisposable, IEnumerable<string>
    {
        #region Private members

        private Stream stream;
        private readonly List<string> files;
        private readonly byte[] header = new byte[sizeof(int) * 2];
        private readonly string baseFileName;
        private readonly long fileMaxSize;
        private long compressedLength;
        private long uncompressedLength;
        private int frameCount;
        private int frameMaxSize;
        private bool disposed;

        private bool useCompression;

        #endregion

        #region Private methods

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="baseFileName">The base name of the file(s) to generate content to.</param>
        /// <param name="maxSize">The max size of each file that will be generated.</param>
        /// <param name="useCompression"></param>
        public BlobContentWriter(string baseFileName, long maxSize, bool useCompression = true)
        {
            if (String.IsNullOrEmpty(baseFileName)) throw new ArgumentNullException("baseFileName");

            files = new List<string>();
            this.baseFileName = baseFileName;
            fileMaxSize = maxSize;
            this.useCompression = useCompression;
            uncompressedLength = 0;
            compressedLength = 0;
            InitializeStream();
        }


        /// <summary>
        /// Creates a new file using the base name and current index and opens a stream on the file.
        /// </summary>
        private void InitializeStream()
        {
            frameCount = 0;
            frameMaxSize = 0;

            files.Add(String.Format("{0}.{1:D3}", baseFileName, files.Count));

            stream = File.Open(files[files.Count - 1], FileMode.Create);
            stream.Write(header, 0, header.Length);
        }

        /// <summary>
        /// Finalizes the file stream by updating the header before closing the stream down.
        /// </summary>
        private void FinalizeStream()
        {
            if (stream != null)
            {
                compressedLength += stream.Length;

                stream.Seek(0, SeekOrigin.Begin);

                var counter = BitConverter.GetBytes(frameCount);
                var largestFrame = BitConverter.GetBytes(frameMaxSize);

                stream.Write(counter, 0, counter.Length);
                stream.Write(largestFrame, 0, largestFrame.Length);

                stream.Dispose();
                stream = null;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">True if managed resources should be cleaned up.</param>
        private void Dispose(bool disposing)
        {
            if (!disposing || disposed) return;

            try
            {
                FinalizeStream();
            }
            finally
            {
                disposed = true;
            }
        }

        #endregion

        #region Constructor and destructor


        /// <summary>
        /// Destructor.
        /// </summary>
        ~BlobContentWriter()
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
        /// Writes a new frame of content to the stream and returns the index of the frame. If max size is greater than
        /// zero, then file splitting will be used.
        /// </summary>
        /// <param name="content">The frame content to store.</param>
        /// <returns>The index of the frame, or (-1) if no content was passed.</returns>
        public int Write(string content)
        {
            if (String.IsNullOrEmpty(content)) return (-1);

            if ((fileMaxSize > 0) && (stream.Position > (fileMaxSize + header.Length)))
            {
                FinalizeStream();
                InitializeStream();
            }

            var encoded = Encoding.ASCII.GetBytes(content);

            if (useCompression)
                encoded = Zip.Compress(encoded);


            frameMaxSize = Math.Max(encoded.Length, frameMaxSize);

            var size = BitConverter.GetBytes(encoded.Length);

            stream.Write(size, 0, size.Length);
            stream.Write(encoded, 0, encoded.Length);

            uncompressedLength += (content.Length + size.Length);

            return frameCount++;
        }

        /// <summary>
        /// Returns enumerator for the files list.
        /// </summary>
        /// <returns>The files list enumerator</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Returns enumerator for the files list.
        /// </summary>
        /// <returns>The files list enumerator</returns>
        public IEnumerator<string> GetEnumerator()
        {
            return files.GetEnumerator();
        }

        /// <summary>
        /// Closes the file stream.
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Returns the name of the file from the file list using the specified index.
        /// </summary>
        public string Files(int index)
        {
            return files[index];
        }

        #endregion

        #region Public properties

        /// <summary>
        /// Returns the name of the file from the file list using the specified index.
        /// </summary>
        public string this[int index]
        {
            get
            {
                return files[index];
            }
        }

        /// <summary>
        /// Returns the number of generated files.
        /// </summary>
        public int FileCount
        {
            get
            {
                return files.Count;
            }
        }

        /// <summary>
        /// Returns the total compressed length of the content.
        /// </summary>
        public long CompressedLength
        {
            get
            {
                return stream.Length + compressedLength;
            }
        }

        /// <summary>
        /// Returns the uncompressed length of the total content.
        /// </summary>
        public long UncompressedLength
        {
            get
            {
                return uncompressedLength;
            }
        }

        #endregion
    }
}