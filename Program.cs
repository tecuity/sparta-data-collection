using System;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace sync_geotab
{
    class Program
    {
        static Task Main(string[] args) => LoadData(args);

        static string getSettingValue(string key)
        {
            string connStr = Properties.Settings.Default.dbConnectionString;
            MySqlConnection conn = new MySqlConnection(connStr);

            conn.Open();

            string sql = "select value from settings where setting_key = 'exceptionToken'";

            MySqlCommand cmd = new MySqlCommand(sql, conn);
            string value = (string) cmd.ExecuteScalar();

            return value;
        }
        static void setSettingsValue(string key, string value)
        {
            string connStr = Properties.Settings.Default.dbConnectionString;
            MySqlConnection conn = new MySqlConnection(connStr);

            conn.Open();

            string sql = "update settings set value = '" + value + "' where setting_key = '" + key + "'";

            MySqlCommand cmd = new MySqlCommand(sql, conn);
            cmd.ExecuteNonQuery();
        }

        static async Task LoadData(String[] args)
        {
            Console.WriteLine("Beginning Execution");

            string database = Properties.Settings.Default.geo_database;
            string server = Properties.Settings.Default.geo_server;
            string user = Properties.Settings.Default.geo_user;
            string password = Properties.Settings.Default.geo_password;
            string path = Properties.Settings.Default.geo_export_path;

            Console.WriteLine("server: " + server);
            Console.WriteLine("database: " + database);
            Console.WriteLine("user: " + user);

            Console.WriteLine("Getting Exception Token Setting");
            long exceptionToken = long.Parse(getSettingValue("exceptionToken"));
            Console.WriteLine(exceptionToken);
            Worker geotabFeed = new DatabaseWorker(user, password, database, server, null, null, null, null, exceptionToken, path);
            Console.WriteLine("Retrieving Geotab data");
            await geotabFeed.DoWorkAsync(false);

            Console.WriteLine("Updating Exception Token");
            setSettingsValue("exceptionToken", geotabFeed.feedParameters.LastExceptionToken.ToString());

            return;
        }
    }
}
