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
            SqliteConnection connection =    new SqliteConnection(@"Data Source=mydb.db");
          
            connection.Open();
            System.Console.WriteLine("creatings new table");
            try
            {
                ExecuteNonSelectStatementWithTransaction(connection, "create table Companies(ID INTEGER  PRIMARY KEY,Title varchar(255)  ,Country varchar(255), AddedDate Date not null);");

            }
            catch (Microsoft.Data.Sqlite.SqliteException)
            {

                System.Console.WriteLine("database  already exist ");
            }
            System.Console.WriteLine("Add to records  10 to db");
            // we can  pass parameters to query in simplier way  using   $"insert into Companies(Title,Country,AddedDate) values({value1},{value2},{value3})"
            // but i think you expect that we should use   method addWithParameters 
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
            var transaction = connection.BeginTransaction();
            string query = "insert into Companies(Title,Country,AddedDate) values(\"Title\",\"Country\",\"AddedDate\")";
            string msg = "enter values as json data {Title:\"str\",Country:\"Str\",AddedDate:\"YYYY-MM-DD\"}";
            System.Console.WriteLine("press 'q' to end session");
            System.Console.WriteLine(query);
            System.Console.WriteLine(msg);
            var line = System.Console.ReadLine();
            while (true)
            {
                if (!string.IsNullOrEmpty(line) && line[0] == 'q'){
                    transaction.Commit();
                    break;
                } 


                try
                {
                    dynamic res = JsonConvert.DeserializeObject<Company>(line);
                    ExecuteNonSelectStatementWithTransaction(connection, transaction, query, res.Title, res.Country, res.AddedDate);
                    System.Console.WriteLine("success");

                }
                catch (Microsoft.Data.Sqlite.SqliteException ex)
                {


                    System.Console.Error.WriteLine(ex.StackTrace);
                    //dont sure when we should do rollback
                    //when we have json parse error or sql insertion error.
                    // i think when we have sql exeption   next insertion can damage our data 
                    transaction.Rollback();
                    System.Console.Error.WriteLine("roll back data");
                    //dont sure what we should do in this case  end session with break  or create new transaction  and continue 
                    transaction = connection.BeginTransaction();
                }
                catch(Exception ex)
                {
                    System.Console.WriteLine(ex.Message);
                }
                System.Console.WriteLine(query);
                System.Console.WriteLine(msg);
                line = Console.ReadLine();
            }

            System.Console.WriteLine("Session ended");
        }
        public static void ExecuteNonSelectStatementWithTransaction(SqliteConnection dbConnection, SqliteTransaction transaction, string query, params dynamic[] parameters)
        {
            SqliteCommand command = dbConnection.CreateCommand();
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
            command.ExecuteNonQuery();
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