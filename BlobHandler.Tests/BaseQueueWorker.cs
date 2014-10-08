using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobHandler.Tests
{
    public abstract class BaseQueueWorker<T> :IWorker
    {
        public dynamic Execute(dynamic data)
        {

            var connectionData = data as ConnectionWorkflowWorkerData;







        }

        public abstract void Process (IEnumerable<T> data);
    }
}
