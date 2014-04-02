using System;

namespace ConsoleApplication2
{
    /// <summary>
    /// The Response handler interface. 
    /// </summary>
    public interface IResponseHandler
    {
        /// <summary>
        /// Handle the response payload for the given requestId.  Send the information
        /// up to the cloud. 
        /// </summary>
        void HandleResponse(Guid requestId, string payload, bool completed);
    }
}