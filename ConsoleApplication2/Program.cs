using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ConsoleApplication2
{
    /// <summary>
    /// Prototype program.
    /// </summary>
    static class Program
    {
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

            Utility.GenerateContent(count);
            Utility.UploadContent(GetBlobClient());
            Utility.DownloadStream(GetBlobClient());

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
            return client;
        }
    }
}
