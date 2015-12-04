using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MySQLAPI
{
    public class MySqlApi
    {
        private string _connectionstring { get; set; }
        private string _userName { get; set; }
        private string _password { get; set; }
        private int _port { get; set; }
        private string _server { get; set; }
        private string _database { get; set; }

        public MySqlApi(string server, int port, string username, string password)
        {
            _connectionstring = string.Format("Server={0}; Port=[1}; Uid={2}; Pwd={3};", server, port, username, password);
            _server = server;
            _port = port;
            _userName = username;
            _password = password;
        }


        /// <summary>
        /// Wraps an SQL connection in a closure to allow minimal typing
        /// </summary>
        /// <param name="body"></param>
        private void SqlConnection(Action<MySqlCommand> body)
        {
            // automatically opens and closes a connection
            using (var conn = new MySqlConnection(_connectionstring))
            {
                conn.Open();
                
                var cmd = conn.CreateCommand();
                body(cmd); // the body of the lambda using the cmd (see use below)
                conn.Close();
            }
        }


        /// <summary>
        /// Opens a connection to the Database, and send the text string
        /// </summary>
        /// <param name="text">the string to send to the database</param>
        /// <returns>the string that was send to the database</returns>
        public string SendString(string text)
        {
            // exposes the connection command in the lambda
            SqlConnection(cmd =>
            {
                    cmd.CommandText = text;
                    cmd.ExecuteNonQuery();
            });

            return text;
        }

        /// <summary>
        /// Gets a string back from the database by sending the text
        /// </summary>
        /// <param name="text">the text thats get send to the database</param>
        /// <returns>The return string from the database.</returns>
        public List<string> GetString(string text)
        {
            List<string> readertext = new List<string>();

            SqlConnection(cmd =>
            {
                cmd.CommandText = text;
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)//jeg er ikek sikker på at fieldcount returnerer antallet af felter/kolonner
                    {
                        readertext.Add(reader.GetString(i));
                    }

                }
            });

            return readertext;
        }
    

        /// <summary>
        /// Select a singel collum, where keycollum = key
        /// </summary>
        /// <param name="table">the name of the table</param>
        /// <param name="targetColumn">The collum you wants data from</param>
        /// <param name="keyColumn">the collum you know</param>
        /// <param name="key">the value you know</param>
        /// <returns>Data from the database</returns>
        public List<string> Select(string table, string targetColumn, string keyColumn, string key)
        {
            string text = String.Format("SELECT {0} FROM {1} WHERE {2} = '{3}'",
                targetColumn, table, keyColumn, key);
            return GetString(text);
        }

        /// <summary>
        /// Inserts alle the data in alle the collum in the speciafic table
        /// </summary>
        /// <param name="table">the name of the table</param>
        /// <param name="column">a list of collums</param>
        /// <param name="value">a list of values</param>
        public void Insert(string table, List<string> column, List<string> value)
        {
            var colStr = String.Join(",", column);
            var valStr = String.Join("','", value);
            var text = String.Format("INSERT INTO {0} ({1}) VALUES('{2}')", table, colStr, valStr);

            SendString(text);
        }

        /// <summary>
        /// Update a line in the database
        /// </summary>
        /// <param name="table">the name of the table</param>
        /// <param name="targetColumn">a list of collums you want to update values</param>
        /// <param name="value">a list of values you ants to update</param>
        /// <param name="keyColumn">the collum of the value you know</param>
        /// <param name="key">the value you know</param>
        public void Update(string table, List<string> targetColumn, List<string> value, string keyColumn, string key)
        {
            if (targetColumn.Count != value.Count) throw new NotEqualException();

            var lrSet = targetColumn.Zip(value, (lhs, rhs) => lhs + " = '" + rhs + "'");
            var resStr = String.Join(",", lrSet);

            String text = String.Format("UPDATE {0} SET {3} WHERE {1} = '{2}'", table, keyColumn, key, resStr);

            SendString(text);
        }

        /// <summary>
        /// Gets a list of list of data from the database
        /// </summary>
        /// <param name="text">the text to send to the database</param>
        /// <param name="columnCount">the number of collums you want back</param>
        /// <returns>a list of list of data</returns>
        public List<List<string>> GetList(string text)
        {
            var readerList = new List<List<string>>();

            SqlConnection(cmd =>
            {
                cmd.CommandText = text;
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    readerList.Add(new List<string>());
                    for (int i = 0; i < reader.FieldCount; i++) //igen jeg ved ikkeo m fieldcount gør det jeg forventer
                    {
                        readerList.Last().Add(reader.GetString(i));
                    }
                }
            });

            return readerList;
        }


        /// <summary>
        /// Filter a list
        /// </summary>
        /// <param name="table">the name of the table</param>
        /// <param name="keyColumn">the know collum</param>
        /// <param name="key">the known value</param>
        /// <param name="numbersOfCollums">numbers of collums you want back</param>
        /// <returns></returns>
        public List<List<string>> FilterList(string table, string keyColumn, string key)
        {
            string text = "SELECT * FROM " + table + " WHERE " + keyColumn + " = '" + key + "'";
            return GetList(text);
        }

        /// <summary>
        /// Select all the data in the database
        /// </summary>
        /// <param name="table">the name of the table</param>
        /// <param name="columnCount">the numbes of collums you wants back</param>
        /// <returns></returns>
        public List<List<string>> SelectAll(string table)
        {
            string text = String.Format("SELECT * FROM {0}", table);
            return GetList(text);
        }

        /// <summary>
        /// Delete a row in the database
        /// </summary>
        /// <param name="table">the table where the row is</param>
        /// <param name="keyColumn"> the known collum</param>
        /// <param name="key">the known value</param>
        public void Delete(string table, string keyColumn, string key)
        {
            string text = "DELETE FROM " + table + " WHERE " + keyColumn + " = '" + key + "'";
            SendString(text);
        }


        /// <summary>
        /// Get Coolumn Names
        /// </summary>
        /// <param name="table">the tabel you want names from</param>
        /// <returns></returns>
        public List<string> GetColumnName(string table)
        {
            string text = "SHOW Columns FROM " + table;
            return GetList(text).Select(e => e[0]).ToList();
        }

        public void CreateTable(string tableName,List<string> columnNames, List<string> valueTypes)
        {
            var set = columnNames.Zip(valueTypes, (lhs, rhs) => lhs + " " + rhs);
            string zipped = string.Join(",", set);
            SendString("CREATE TABLE '" + tableName + "'(" + zipped + ")");
        }

        public void SetDatabase(string databaseName)
        {
            _connectionstring = string.Format("Server={0}; Port={1}; Database={2}; Uid={3}; Pwd={4};", _server, _port, _database, _userName, _password);
        }

        public void UnsetDatabase()
        {
            _connectionstring = string.Format("Server={0}; Port={1}; Uid={3}; Pwd={4};", _server, _port, _userName, _password);
        }

    }
}
