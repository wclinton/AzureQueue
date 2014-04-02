using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ConsoleApplication2
{
    /// <summary>
    /// Class for handling creation of a multi-frame file to upload to blob storage.
    /// </summary>
    public sealed class BlobContentWriter : IDisposable, IEnumerable<string>
    {
        #region Private members

        private Stream _stream;
        private readonly List<string> _files;
        private readonly byte[] _header = new byte[sizeof(int) * 2];
        private readonly string _baseFileName;
        private readonly long _fileMaxSize;
        private long _compressedLength;
        private long _uncompressedLength;
        private int _frameCount;
        private int _frameMaxSize;
        private bool _disposed;

        #endregion

        #region Private methods
    
        /// <summary>
        /// Creates a new file using the base name and current index and opens a stream on the file.
        /// </summary>
        private void InitializeStream()
        {
            _frameCount = 0;
            _frameMaxSize = 0;

            _files.Add(String.Format("{0}.{1:D3}", _baseFileName, _files.Count));

            _stream = File.Open(_files[_files.Count - 1], FileMode.Create);
            _stream.Write(_header, 0, _header.Length);
        }

        /// <summary>
        /// Finalizes the file stream by updating the header before closing the stream down.
        /// </summary>
        private void FinalizeStream()
        {
            if (_stream != null)
            {
                _compressedLength += _stream.Length;

                _stream.Seek(0, SeekOrigin.Begin);

                var counter = BitConverter.GetBytes(_frameCount);
                var largestFrame = BitConverter.GetBytes(_frameMaxSize);

                _stream.Write(counter, 0, counter.Length);
                _stream.Write(largestFrame, 0, largestFrame.Length);

                _stream.Dispose();
                _stream = null;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">True if managed resources should be cleaned up.</param>
        private void Dispose(bool disposing)
        {
            if (!disposing || _disposed) return;

            try
            {
                FinalizeStream();
            }
            finally
            {
                _disposed = true;
            }
        }

        #endregion

        #region Constructor and destructor

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="baseFileName">The base name of the file(s) to generate content to.</param>
        /// <param name="maxSize">The max size of each file that will be generated.</param>
        public BlobContentWriter(string baseFileName, long maxSize)
        {
            if (String.IsNullOrEmpty(baseFileName)) throw new ArgumentNullException("baseFileName");

            _files = new List<string>();
            _baseFileName = baseFileName;
            _fileMaxSize = maxSize;
            _uncompressedLength = 0;
            _compressedLength = 0;

            InitializeStream();
        }

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

            if ((_fileMaxSize > 0) && (_stream.Position > (_fileMaxSize + _header.Length)))
            {
                FinalizeStream();
                InitializeStream();
            }

            var compressed = Zip.Compress(Encoding.ASCII.GetBytes(content));

            _frameMaxSize = Math.Max(compressed.Length, _frameMaxSize);

            var size = BitConverter.GetBytes(compressed.Length);

            _stream.Write(size, 0, size.Length);
            _stream.Write(compressed, 0, compressed.Length);

            _uncompressedLength += (content.Length + size.Length);

            return _frameCount++;
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
            return _files.GetEnumerator();
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
            return _files[index];
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
                return _files[index];
            }
        }

        /// <summary>
        /// Returns the number of generated files.
        /// </summary>
        public int FileCount
        {
            get
            {
                return _files.Count;
            }
        }

        /// <summary>
        /// Returns the total compressed length of the content.
        /// </summary>
        public long CompressedLength
        {
            get
            {
                return _stream.Length + _compressedLength;
            }
        }

        /// <summary>
        /// Returns the uncompressed length of the total content.
        /// </summary>
        public long UncompressedLength
        {
            get
            {
                return _uncompressedLength;
            }
        }

        #endregion
    }
}