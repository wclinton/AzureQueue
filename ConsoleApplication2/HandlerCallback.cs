using System;

namespace ConsoleApplication2
{
    /// <summary>
    /// Callback handler
    /// </summary>
    public class HandlerCallback : IResponseHandler
    {
        private BlobContentWriter _writer;

        public HandlerCallback()
        {
            _writer = new BlobContentWriter("invoices.json", 128); ///*32 */ 1024 * 1000);
        }

        public void HandleResponse(Guid requestId, string payload, bool completed)
        {
            Console.Write(".");

            _writer.Write(payload);

            if (!completed) return;

            double value = ((_writer.UncompressedLength - _writer.CompressedLength) / (Double)_writer.UncompressedLength) * 100;

            Console.WriteLine();
            Console.WriteLine("Uncompressed size: {0:N0}", _writer.UncompressedLength);
            Console.WriteLine("Compressed size:   {0:N0}", _writer.CompressedLength);
            Console.WriteLine("Compression:       {0:N2}%", value);

            _writer.Dispose();
            _writer = null;
        }
    }
}