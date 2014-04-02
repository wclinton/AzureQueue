using System.Collections;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConsoleApplication2
{
    internal class BlobsContextReader<T> : IEnumerable<T>, IEnumerator<T>
    {
        private readonly IEnumerator<ICloudBlob> blobsEnumerator;
        private IEnumerator<T> currentEnumerator;


        private int boundedCapacity;



        public BlobsContextReader(IEnumerable<ICloudBlob> blobs, int boundedCapacity = 128)
        {

            blobsEnumerator = blobs.GetEnumerator();
            var currentBlob = GetNextBlob();
            var currentReader = new BlobContentReader<T>(currentBlob, boundedCapacity);

            currentEnumerator = currentReader.GetEnumerator();

            this.boundedCapacity = boundedCapacity;
        }


        private IEnumerator<T> GetNextEnumerator()
        {
            //get the next blob ...
            var currentBlob = GetNextBlob();

            if (currentBlob == null) return null;

            var currentReader = new BlobContentReader<T>(currentBlob, boundedCapacity);

            currentEnumerator = currentReader.GetEnumerator();

            return currentEnumerator;
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
            if (currentEnumerator.MoveNext())
                return true;
            //we got to the end of this blob

            currentEnumerator = GetNextEnumerator();

            if (currentEnumerator == null)
                return false;

            return true;
        }

        public void Reset()
        {
           blobsEnumerator.Reset();
           var currentBlob = GetNextBlob();
           var currentReader = new BlobContentReader<T>(currentBlob, boundedCapacity);
           currentEnumerator = currentReader.GetEnumerator();
        }

        public T Current
        {
            get { return currentEnumerator.Current; }            
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }
    }
}