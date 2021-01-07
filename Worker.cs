using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sync_geotab
{
    abstract class Worker
    {
        readonly string path;
        bool stop;
        internal FeedResultData results;
        internal FeedParameters feedParameters;
        internal FeedProcessor feedService;
        /// <summary>
        /// Initializes a new instance of the <see cref="Worker"/> class.
        /// </summary>
        /// <param name="path">The path.</param>
        internal Worker(string path)
        {
            this.path = path;
        }

        /// <summary>
        /// Displays the feed results.
        /// </summary>
        /// <param name="results">The results.</param>
        public async Task DisplayFeedResultsAsync(FeedResultData results)
        {
            // Output to console
            // new FeedToConsole(results.GpsRecords, results.StatusData, results.FaultData).Run();
            // Optionally we can output to csv or google doc:
            new FeedToCsv(path, results.GpsRecords, results.StatusData, results.FaultData, results.Trips, results.ExceptionEvents).Run();

            // new FeedToBigquery(path).Run();
            await Task.Delay(1000);
        }

        public async Task FeedResultToMySQL(FeedResultData results)
        {
            // Output to console
            // new FeedToConsole(results.GpsRecords, results.StatusData, results.FaultData).Run();
            // Optionally we can output to csv or google doc:
            new FeedToMySql(results.GpsRecords, results.StatusData, results.FaultData, results.Trips, results.ExceptionEvents).Run();

            // new FeedToBigquery(path).Run();
            await Task.Delay(1000);
        }

        /// <summary>
        /// Do the work.
        /// </summary>
        /// <param name="obj">The object.</param>
        public async Task DoWorkAsync(bool continuous)
        {
            do
            {
                await WorkActionAsync();
            }
            while (continuous && !stop);
        }

        /// <summary>
        /// Requests to stop.
        /// </summary>
        public void RequestStop()
        {
            stop = true;
        }

        /// <summary>
        /// The work action.
        /// </summary>
        public abstract Task WorkActionAsync();

    }
}
