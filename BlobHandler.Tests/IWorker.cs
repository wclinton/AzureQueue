using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobHandler.Tests
{
    interface IWorker
    {
        dynamic Execute(dynamic data);
    }
}
