using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ConsoleApplication2;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Nephos.Model;
using Utility;

namespace BlobHandler.Tests
{
    [TestClass]
    public class BlobContentReaderTests
    {
        [TestMethod]
        public void CanReadBlobContent()
        {
            const int count = 1000;

            var originalItems = GenerateContent(count);
            StoreContent(originalItems);

            var blobs = UploadContent(GetBlobClient());
            var items = DownloadStream(blobs);


            Assert.AreEqual(count, originalItems.Count);
            Assert.AreEqual(originalItems.Count, items.Count);

            for (var i = 0; i < count; i++)
            {

                Assert.AreEqual(originalItems[i].ExternalId, items[i].ExternalId);
                Assert.AreEqual(originalItems[i].ExternalReference, items[i].ExternalReference);
            }
        }

        private static CloudBlobClient GetBlobClient()
        {
            var cloudStorageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            var client = cloudStorageAccount.CreateCloudBlobClient();
            return client;
        }


        private static IEnumerable<ICloudBlob> UploadContent(CloudBlobClient client)
        {
            var container = client.GetContainerReference(AzureAccount.Container);

            container.CreateIfNotExists();

            // Set permissions on the container.
            var containerPermissions = new BlobContainerPermissions {PublicAccess = BlobContainerPublicAccessType.Blob};
            // This sample sets the container to have public blobs. Your application
            // needs may be different. See the documentation for BlobContainerPermissions
            // for more information about blob container permissions.

            // MESSAGE SENT
            container.SetPermissions(containerPermissions);

            //Delete all existing blobs in the container.
            foreach (var cloudBlob in container.ListBlobs().Select(blob => container.GetBlockBlobReference(blob.Uri.ToString())))
            {
                cloudBlob.DeleteIfExists();
            }          


            var files = from f in Directory.EnumerateFiles(Directory.GetCurrentDirectory())
                        where f.Contains("invoices.json.")
                        select f;

            var i = 0;

            var blobList = new List<ICloudBlob>();
            foreach (var path in files)
            {
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read))
                {
                    var blob = container.GetBlockBlobReference("blob" + i.ToString(("D3")));
                    i++;
                    try
                    {
                        blob.Delete();
                        Console.WriteLine("Deleted existing blob...");
                    }
                    // ReSharper disable once EmptyGeneralCatchClause
                    catch (Exception)
                    {

                    }


                    blob.ServiceClient.ParallelOperationThreadCount = 4;
                    blob.UploadFromStream(stream);
                    blobList.Add(blob);
                }
            }        

            return blobList;
        }

        private static List<Invoice> DownloadStream(IEnumerable<ICloudBlob> blobList)
        {
            var invoiceList = new List<Invoice>();

            using (var reader = new BlobsContextReader<Invoice>(blobList))
            {
                invoiceList.AddRange(reader);
            }

            return invoiceList;
        }

        private static List<Invoice> GenerateContent(int count)
        {
            var invoiceList = new List<Invoice>();

            for (var i = 0; i < count; i++)
            {
                var invoice = InvoiceUtil.CreateInvoice(i);

                invoiceList.Add(invoice);
            }
            return invoiceList;
        }

        private static void StoreContent(IEnumerable<Invoice> invoiceList)
        {
            var files = from f in Directory.EnumerateFiles(Directory.GetCurrentDirectory())
                        where f.Contains("invoices.json.")
                        select f;

            foreach (var file in files)
            {
                File.Delete(file);
            }

            var callback = new HandlerCallback();

            var streamer = new JsonContentListStreamer(null, callback, Guid.NewGuid());

            foreach (var invoice in invoiceList)
            {                       
                streamer.Add(invoice);               
            }
            streamer.Flush(true);         
        }
    }
}
