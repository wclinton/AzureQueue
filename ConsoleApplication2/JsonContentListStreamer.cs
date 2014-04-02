using System;
using System.Text;
using Newtonsoft.Json;

namespace ConsoleApplication2
{
    /// <summary>
    /// Class for generating serialized JSON list content based on objects or pre serialized data.
    /// </summary>
    public class JsonContentListStreamer
    {
        #region Private memebers

        private const char JsonArrayStartToken = '[';
        private const char JsonArrayEndToken = ']';
        private const char JsonSeperatorToken = ',';

        private readonly JsonSerializerSettings _settings;
        private readonly IResponseHandler _handler;
        private readonly StringBuilder _content;
        private readonly Guid _id;
        private readonly long _maxContentSize;
        private int _count;

        #endregion

        #region Private methods

        /// <summary>
        /// Prepares the content stream for the next grouping of objects.
        /// </summary>
        private void InitializeStream()
        {
            _count = 0;
            _content.Clear();
            _content.Append(JsonArrayStartToken);
        }

        /// <summary>
        /// Finalizes the content stream in preperation for sending to the callback.
        /// </summary>
        private void FinalizeStream()
        {
            _content.Append(JsonArrayEndToken);
        }

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings">The serializer settings to use. If null the default settings will be used.</param>
        /// <param name="handler">The callback handler to recieve content when the size threshold has been reached.</param>
        /// <param name="id">The identifier to associate with this content streamer.</param>
        /// <param name="maxContentSize">The threshold size that will trigger a callback.</param>
        public JsonContentListStreamer(JsonSerializerSettings settings, IResponseHandler handler, Guid id, long maxContentSize = 1024 * 1000)
        {
            if (handler == null) throw new ArgumentNullException("handler");
            if (maxContentSize < 1) throw new ArgumentOutOfRangeException("maxContentSize");

            _settings = settings ?? new JsonSerializerSettings {ReferenceLoopHandling = ReferenceLoopHandling.Ignore};
            _content = new StringBuilder(Convert.ToInt32(maxContentSize));
            _maxContentSize = maxContentSize;
            _handler = handler;
            _id = id;

            InitializeStream();
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Adds new serialized content to the content stream.
        /// </summary>
        /// <param name="content">The serialized content to append to the stream.</param>
        public void Add(string content)
        {
            if (String.IsNullOrEmpty(content)) return;

            if (((_content.Length + content.Length) > _maxContentSize) && (_count > 0)) Flush(); 
            if (_count++ > 0) _content.Append(JsonSeperatorToken);

            _content.Append(content);
        }

        /// <summary>
        /// Serializes the object to a JSON string and appends the result to the content stream.
        /// </summary>
        /// <param name="content">The object to serialize to the content stream.</param>
        public void Add(object content)
        {
            if (content != null) Add(JsonConvert.SerializeObject(content, Formatting.None, _settings));
        }

        /// <summary>
        /// Clears the current content stream.
        /// </summary>
        public void Clear()
        {
            InitializeStream();
        }

        /// <summary>
        /// Finalizes the current content stream and performs the handler callback.
        /// </summary>
        public void Flush(bool completed = false)
        {
            FinalizeStream();

            try
            {
                if (_handler != null)
                {
                    _handler.HandleResponse(_id, _content.ToString(), completed);
                }
            }
            finally
            {
                InitializeStream();
            }
        }

        #endregion

        #region Public properties

        /// <summary>
        /// Returns the id associated with the content stream.
        /// </summary>
        public Guid Id
        {
            get
            {
                return _id;
            }
        }

        /// <summary>
        /// Returns the maximum content length for the stream.
        /// </summary>
        public long MaxContentSize
        {
            get
            {
                return _maxContentSize;
            }
        }

        /// <summary>
        /// Returns the current length of the content stream.
        /// </summary>
        public int ContentSize
        {
            get
            {
                return _content.Length;
            }
        }

        /// <summary>
        /// Returns the count of content items (that have not been flushed).
        /// </summary>
        public int Count
        {
            get
            {
                return _count;
            }
        }

        #endregion
    }
}