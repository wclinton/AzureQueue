using System.Collections;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConsoleApplication2
{
    internal class BlobsContextReader<T> : IEnumerable<T>, IEnumerator<T>
    {
        private readonly IEnumerator<ICloudBlob> blobsEnumerator;
        private IEnumerator<T> masterEnumerator;
        private readonly int boundedCapacity;


        public BlobsContextReader(IEnumerable<ICloudBlob> blobs, int boundedCapacity = 128)
        {
            this.boundedCapacity = boundedCapacity;
            blobsEnumerator = blobs.GetEnumerator();
            masterEnumerator = GetNextItemEnumerator();
        }

        private IEnumerator<T> GetNextItemEnumerator()
        {
            //get the next blob ...
            var blob = GetNextBlob();
            return blob == null ? null : GetItemEnumerator(blob);
        }

        private IEnumerator<T> GetItemEnumerator(ICloudBlob blob)
        {         
            var reader = new BlobContentReader<T>(blob, boundedCapacity);
            return reader.GetEnumerator();            
        }

        

        private ICloudBlob GetNextBlob()
        {
            if (blobsEnumerator.MoveNext())
                return blobsEnumerator.Current;
            return null;
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
            if (masterEnumerator.MoveNext())
                return true;


            //we got to the end of this blob - try the next one.
            masterEnumerator = GetNextItemEnumerator();

            if (masterEnumerator == null) return false;
            return masterEnumerator.MoveNext();            
        }

        public void Reset()
        {
           blobsEnumerator.Reset();
           masterEnumerator = GetItemEnumerator(blobsEnumerator.Current);
        }

        public T Current
        {
            get { return masterEnumerator.Current; }            
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }
    }
}