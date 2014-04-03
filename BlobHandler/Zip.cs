using System.IO;
using System.IO.Compression;

namespace BlobHandler
{
    /// <summary>
    /// Static compression class.
    /// </summary>
    public static class Zip
    {
        /// <summary>
        /// Decompresses an array of byte and returns the uncompressed byte array.
        /// </summary>
        /// <param name="compressedData">The compressed data.</param>
        /// <param name="size">The number of bytes to decompress from the buffer.</param>
        /// <returns>The decompressed array of bytes.</returns>
        public static byte[] Decompress(byte[] compressedData, int size = (-1))
        {
            using (var outputStream = new MemoryStream())
            {
                using (var inputStream = size == (-1) ? new MemoryStream(compressedData) : new MemoryStream(compressedData, 0, size))
                {
                    using (var zip = new GZipStream(inputStream, CompressionMode.Decompress))
                    {
                        zip.CopyTo(outputStream);
                    }
                }

                return outputStream.ToArray();
            }
        }

        /// <summary>
        /// Compresses an array of byte and returns the compressed byte array.
        /// </summary>
        /// <param name="data">The data to compress.</param>
        /// <returns>The compressed array of bytes.</returns>
        public static byte[] Compress(byte[] data)
        {
            using (var outputStream = new MemoryStream())
            {
                using (var zip = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    zip.Write(data, 0, data.Length);
                }

                return outputStream.ToArray();
            }
        }
    }
}