using System.Collections;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobHandler
{
    public class BlobsContextReader<T> : IEnumerable<T>, IEnumerator<T>
    {
        private readonly IEnumerator<ICloudBlob> blobsEnumerator;
        private IEnumerator<T> itemEnumerator;
        private readonly int boundedCapacity;

        public BlobsContextReader(IEnumerable<ICloudBlob> blobs, int boundedCapacity = 128)
        {
            this.boundedCapacity = boundedCapacity;
            blobsEnumerator = blobs.GetEnumerator();
            var blob = GetNextBlob();
            itemEnumerator = GetItemEnumerator(blob);
        }

        private ICloudBlob GetNextBlob()
        {
            if (blobsEnumerator.MoveNext())
                return blobsEnumerator.Current;
            return null;
        }

        private IEnumerator<T> GetItemEnumerator(ICloudBlob blob)
        {
            var reader = new BlobContentReader<T>(blob, boundedCapacity);
            return reader.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }


        public void Dispose()
        {
            //   throw new System.NotImplementedException();
        }

        public bool MoveNext()
        {
            //If we can - just move the current enumerator...
            if (itemEnumerator.MoveNext())
                return true;

            //we got to the end of this blob - try the next one.
            var blob = GetNextBlob();

            if (blob == null)
                return false;

            itemEnumerator = GetItemEnumerator(blob);

            if (itemEnumerator == null) return false;
            return itemEnumerator.MoveNext();
        }

        public void Reset()
        {
            blobsEnumerator.Reset();
            var blob = GetNextBlob();
            itemEnumerator = GetItemEnumerator(blob);
        }

        public T Current
        {
            get { return itemEnumerator.Current; }
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }
    }
}