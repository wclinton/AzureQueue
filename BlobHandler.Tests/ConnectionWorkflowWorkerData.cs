using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobHandler.Tests
{
    class ConnectionWorkflowWorkerData
    {
        public int TenantId { get; set; }

        public List<Uri> BlobUris { get; set; }
    }
}
