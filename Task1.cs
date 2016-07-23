using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using System.Data;
using Newtonsoft.Json;
using System.IO;


namespace Task1
{


    class Db
    {
        public static void Main()
        {
            System.Console.WriteLine("creating db...");
            File.Create("mydb.db");


            System.Console.WriteLine("Getting Connection to db");
            SqliteConnection connection = GetConnection(@"Data Source=mydb.db");
            connection.Open();
            System.Console.WriteLine("creatings new table");
            try
            {
                ExecuteNonSelectStatementWithTransaction(connection, "create table Companies(ID INTEGER  PRIMARY KEY,Title varchar(255)  ,Country varchar(255), AddedDate Date not null);");

            }
            catch (Microsoft.Data.Sqlite.SqliteException)
            {

                System.Console.WriteLine("database exist ");
            }
            System.Console.WriteLine("Add to records  10 to db");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Luxoft", "Ukraine", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Global Logic", "Ukraine", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Aspera", "Ukraine", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Atom", "Ukraine", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "NI", "Ukraine", "1997-01-01");

            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Astra", "US", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Sigma Soft", "Sweden", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Omikron", "Sweden", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Facebook", "US", "1997-01-01");
            ExecuteNonSelectStatementWithTransaction(connection, "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)", "Google", "US", "1997-01-01");

            System.Console.WriteLine("select max id");

            ExecuteSelectStatementAndPrint(connection, "Select ID,Title from Companies where id= (select max(id) from Companies)");

            System.Console.WriteLine("update values change Ukraine to Us ");
            ExecuteNonSelectStatementWithTransaction(connection, "UPDATE Companies set Country=$v1 where Country=$v2", "US", "Ukraine");

            System.Console.WriteLine("Delete Us countries");
            ExecuteNonSelectStatementWithTransaction(connection, "Delete from Companies where Country=$v1", "US");

            System.Console.WriteLine("Selecting total rows in db");
            ExecuteSelectStatementAndPrint(connection, "Select 'Total rows',count(*) from Companies");

            System.Console.WriteLine("select all data from db");
            ExecuteSelectStatementAndPrint(connection, "Select * from Companies");


            System.Console.WriteLine("starting session with user input");
            StartUserSession(connection);

            connection.Close();
        }




        public static void StartUserSession(SqliteConnection connection)
        {
            string query = "insert into Companies(Title,Country,AddedDate) values($v1,$v2,$v3)";
            string msg = "enter values as json data {Title:\"str\"},Country:\"Str\",AddedDate:\"YYYY-MM-DD\"";
            System.Console.WriteLine("press 'q' to end session");
            System.Console.WriteLine(query);
            System.Console.WriteLine(msg);
            var line = System.Console.ReadLine();
            while (true)
            {
                if (!string.IsNullOrEmpty(line) && line[0] == 'q') break;


                try
                {
                    dynamic res = JsonConvert.DeserializeObject<Company>(line);
                    ExecuteNonSelectStatementWithTransaction(connection, query, res.Title, res.Country, res.AddedDate);
                    System.Console.WriteLine("success");


                }
                catch (Exception ex)
                {
                    System.Console.WriteLine(ex.Message);
                }
                System.Console.WriteLine(query);
                System.Console.WriteLine(msg);
                line = Console.ReadLine();
            }
            System.Console.WriteLine("Session ended");
        }
        public static void ExecuteNonSelectStatementWithTransaction(SqliteConnection dbConnection, string query, params dynamic[] parameters)
        {

            SqliteCommand command = dbConnection.CreateCommand();
            SqliteTransaction transaction = dbConnection.BeginTransaction();
            command.Transaction = transaction;
            command.Connection = dbConnection;
            command.CommandText = query;
            if (parameters != null)
            {
                for (int i = 0, k = 1; i < parameters.Length; i++, k = i + 1)
                {
                    var item = parameters[i];
                    command.Parameters.AddWithValue("$v" + k, item);
                }

            }
            try
            {
                command.ExecuteNonQuery();
                transaction.Commit();
                System.Console.WriteLine("record are writen to db");
            }
            catch (System.Exception)
            {


                try
                {
                    transaction.Rollback();
                    System.Console.WriteLine("rollback data");
                }
                catch (Exception ex2)
                {
                    Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                    Console.WriteLine("  Message: {0}", ex2.Message);
                }
                throw;


            }
            finally
            {
                transaction.Dispose();
            }
        }

        public static void ExecuteSelectStatementAndPrint(SqliteConnection connection, string query)
        {

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = query;
            using (SqliteDataReader reader = selectCommand.ExecuteReader())
            {

                while (reader.Read())
                {
                    var record = reader as IDataRecord;
                    ReadSingleRow(record);
                }
            }
        }
        public static SqliteConnection GetConnection(string connectionParams)
        {
            return new SqliteConnection(connectionParams);
        }

        private static void ReadSingleRow(IDataRecord record)

        {
            string res = "| ";
            for (int i = 0; i < record.FieldCount; i++)
            {
                res += record[i] + " | ";
            }
            Console.WriteLine(res);
            Console.WriteLine("--------------------------------------------");
        }





    }
    public class Company
    {
        public string Title;
        public string Country;
        public string AddedDate;
    }

}