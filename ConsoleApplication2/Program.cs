using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BlobHandler;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Nephos.Model;

namespace ConsoleApplication2
{
    /// <summary>
    /// Prototype program.
    /// </summary>
    static class Program
    {
        static void DownloadStream()
        {
            var client = GetBlobClient();

            var container = client.GetContainerReference(AzureAccount.Container);
      
            long st = Environment.TickCount;

          

            Console.WriteLine("Starting streaming download...");


              

            var blobList = new List<ICloudBlob>();


            foreach (var blob in container.ListBlobs())
            {
                var cloudBlob = container.GetBlockBlobReference(blob.Uri.ToString());
                blobList.Add(cloudBlob);
            }

//            while (!done)
//            {
//
//                var blobName = Directory.GetCurrentDirectory() + "\\invoices.json." + i.ToString("D3");
//
//                try
//                {
//                    blob = container.GetBlobReferenceFromServer(blobName);
//                    blobList.Add(blob);
//                }
//
//                catch
//                {
//                    done = true;
//                    continue;
//                }
//                i++;
//            }

            using (var reader = new BlobsContextReader<Invoice>(blobList))
            {

                foreach (var invoice in reader)
                {

                    //Console.Write(invoice.Dump());

                    Console.WriteLine("invoice: "+invoice.InvoiceNumber+ " Date:"+invoice.InvoiceDate);


                }
                Console.Write(".");
            }        

            Console.WriteLine();
            long et = Environment.TickCount - st;

            Console.WriteLine("Streaming download and deserialization: {0:N0} ms", et);
        }

        static void GenerateContent(int count)
        {
            Console.WriteLine("Starting content generation...");

            var files = from f in Directory.EnumerateFiles(Directory.GetCurrentDirectory())
                        where f.Contains("invoices.json.")
                        select f;

            foreach (var file in files)
            {
                File.Delete(file);
            }

            long st = Environment.TickCount;

            var callback = new HandlerCallback();

            var streamer = new JsonContentListStreamer(null, callback, Guid.NewGuid());

            for (var i = 0; i < count; i++)
            {
                var invoice = InvoiceUtil.CreateInvoice(i);
                streamer.Add(invoice);
            }
            streamer.Flush(true);

            long et = Environment.TickCount - st;

            Console.WriteLine("Content generation time: {0:N0} ms", et);
        }

        static void UploadContent()
        {
            var client = GetBlobClient();

            var container = client.GetContainerReference(AzureAccount.Container);

            container.CreateIfNotExists();

            // Set permissions on the container.
            var containerPermissions = new BlobContainerPermissions();
            // This sample sets the container to have public blobs. Your application
            // needs may be different. See the documentation for BlobContainerPermissions
            // for more information about blob container permissions.
            containerPermissions.PublicAccess = BlobContainerPublicAccessType.Blob;

            // MESSAGE SENT
            container.SetPermissions(containerPermissions);


           
            //Delete all existing blobs in the container.
            foreach (var blob in container.ListBlobs())
            {
                var cloudBlob = container.GetBlockBlobReference(blob.Uri.ToString());
                cloudBlob.DeleteIfExists();
            }



            // blob

            //            try
            //            {
            //                blob.Delete();
            //                Console.WriteLine("Deleted existing blob...");
            //            }
            //            // ReSharper disable once EmptyGeneralCatchClause
            //            catch (Exception)
            //            {
            //
            //            }

            long st = Environment.TickCount;

            Console.WriteLine("Starting blob upload...");


            var files = from f in Directory.EnumerateFiles(Directory.GetCurrentDirectory())
                        where f.Contains("invoices.json.")
                        select f;

            var i = 0;
            foreach (var path in files)
            {
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read))
                {
                    var blob = container.GetBlockBlobReference("blob"+ i.ToString(("D3")));
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
                }
            }

            long et = Environment.TickCount - st;

            Console.WriteLine("Blob upload complete: {0:N0} ms (ParallelOperationThreadCount = 4)", et);
        }

        // ReSharper disable once UnusedParameter.Local
        static void Main(string[] args)
        {
            var count = (-1);

            while (count < 1)
            {
                Console.Write("Enter number of invoices to create (0 to exit): ");
                if (!Int32.TryParse(Console.ReadLine(), out count))
                {
                    count = (-1);
                }
                if (count == 0) return;
            }

            long st = Environment.TickCount;

            GenerateContent(count);
            UploadContent();
            DownloadStream();

            long et = Environment.TickCount - st;

            Console.WriteLine();
            Console.WriteLine("Elapsed time: {0:N0} ms", et);
            Console.WriteLine();

            Console.WriteLine("Press ENTER to quit");
            Console.ReadLine();
        }


        private static CloudBlobClient GetBlobClient()
        {
            var cloudStorageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            var client = cloudStorageAccount.CreateCloudBlobClient();
            //
            //            var credentials = new StorageCredentials(AzureAccount.Name, AzureAccount.Key);
            //            var account = new CloudStorageAccount(credentials, true);
            //            var client = new CloudBlobClient(account.BlobStorageUri, credentials)
            //            {
            //                RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(5), 5)
            //            };

            return client;
        }
    }
}
