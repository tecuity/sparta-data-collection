using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sync_geotab
{
    class DatabaseWorker : Worker
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseWorker" /> class.
        /// </summary>
        /// <param name="user">The user.</param>
        /// <param name="password">The password.</param>
        /// <param name="database">The database.</param>
        /// <param name="server">The server.</param>
        /// <param name="gpsToken">The GPS token.</param>
        /// <param name="statusToken">The status token.</param>
        /// <param name="faultToken">The fault token.</param>
        /// <param name="tripToken">The trip token.</param>
        /// <param name="exceptionToken">The exception token.</param>
        /// <param name="path">The path.</param>
        public DatabaseWorker(string user, string password, string database, string server, long? gpsToken, long? statusToken, long? faultToken, long? tripToken, long? exceptionToken, string path)
            : base(path)
        {
            feedParameters = new FeedParameters(gpsToken, statusToken, faultToken, tripToken, exceptionToken);
            feedService = new FeedProcessor(server, database, user, password);
        }

        /// <summary>
        /// The work action.
        /// </summary>
        /// <inheritdoc />
        public async override Task WorkActionAsync()
        {
            results = await feedService.GetAsync(feedParameters);
            //await DisplayFeedResultsAsync(results);
            await FeedResultToMySQL(results);
        }
    }
}
