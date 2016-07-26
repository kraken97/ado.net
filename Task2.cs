using System;
using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;

namespace Task2
{


    public class Db
    {
        public static void Main(string[] args)
        {


            using (IDbConnection connection = new SqliteConnection(GetConnectionString()))
            {
                connection.Open();
                //qyery1;
                ExecuteSELECTStatementAndPrint(connection, "SELECT  ContactName FROM customers WHERE ContactName like 'D%'");
                //query2
                ExecuteSELECTStatementAndPrint(connection, "SELECT  upper(ContactName) FROM customers");
                //query3;
                ExecuteSELECTStatementAndPrint(connection, "SELECT  distinct Country FROM Customers order by Country");
                //query4
                ExecuteSELECTStatementAndPrint(connection, "SELECT ContactName FROM Customers WHERE City='London' and ContactTitle like 'Sales%' ");

                var query5 = " SELECT  OrderID FROM 'Order Details' as Orders ,Products WHERE  Products.ProductName='Tofu' and  Products.ProductId =Orders.ProductID";
                ExecuteSELECTStatementAndPrint(connection, query5);
                var query6 = @" SELECT ProductName  FROM Orders,Products,'Order Details' as OrderD  
                                        WHERE ShipCountry='Germany' and 
                                        Orders.OrderID=OrderD.OrderID and 
                                        OrderD.ProductID=Products.ProductID ";
                ExecuteSELECTStatementAndPrint(connection, query6);



                var subqueryProductId = "SELECT ProductId FROM Products WHERE ProductName='Ikura'";
                var subqueryCustomerIds = $@"SELECT  CustomerID FROM Orders 
                                            left join  'Order Details' as OrderD  on  Orders.OrderID=OrderD.OrderID 
                                            WHERE ProductId=({subqueryProductId}";
                var query7 = $"SELECT * FROM Customers WHERE CustomerId in ({subqueryCustomerIds}) ) ";
                ExecuteSELECTStatementAndPrint(connection, query7);

                var query8 = " SELECT  *  FROM Employees,Orders  WHERE  Employees.EmployeeID = Orders.EmployeeID ";
                ExecuteSELECTStatementAndPrint(connection, query8);

                var query9 = "SELECT * FROM Employees left join Orders on Employees.EmployeeID=Orders.EmployeeID";
                ExecuteSELECTStatementAndPrint(connection, query9);

                var query10 = "SELECT phone  FROM Shippers union SELECT Phone FROM Suppliers ";
                ExecuteSELECTStatementAndPrint(connection, query10);

                var query11 = "SELECT     City ,count(City) FROM Customers  group by City  order by City";
                ExecuteSELECTStatementAndPrint(connection, query11);

                var query12 = @" SELECT * FROM Customers c 
                                WHERE  17 > (SELECT  AVG(UnitPrice) as PriceAv FROM Orders,'Order Details' as OrderD 
                                WHERE c.CustomerID=Orders.CustomerID and  Orders.OrderID=OrderD.OrderID)";
                ExecuteSELECTStatementAndPrint(connection, query12);

                var query13 = " SELECT Phone FROM Customers WHERE  Phone glob '[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'  ";
                ExecuteSELECTStatementAndPrint(connection, query13);

                var query14 = @" SELECT  CustomerID,max(OrdersN)
                                             FROM  (SELECT CustomerID,count(Orders.CustomerID) as OrdersN FROM Orders group by CustomerID)";
                ExecuteSELECTStatementAndPrint(connection, query14);

                var subqueryFamia = @"SELECT  ProductId FROM 'Order Details' as OrderD  
                                            where OrderD.OrderID in (
                                         Select OrderId  from Orders   
                                                    WHERE CustomerID='FAMIA')";
                var subqueryInner = @"SELECT  ProductId FROM 'Order Details' as OrderD   
                                                where OrderD.orderId  in (select orders.orderid from  Orders  
                                                            WHERE   out.CustomerID=Orders.CustomerID   )";
                var query15 = $@" SELECT CustomerId FROM Customers out 
                                            WHERE   CustomerID !='FAMIA' and  (SELECT count(*) FROM ({subqueryInner} except    {subqueryFamia}))=0";
                ExecuteSELECTStatementAndPrint(connection, query15);


            }


        }
        public static void ExecuteSELECTStatementAndPrint(IDbConnection connection, string query)
        {

            var SELECTCommand = connection.CreateCommand();
            SELECTCommand.CommandText = query;
            using (var reader = SELECTCommand.ExecuteReader())
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
        private static string GetConnectionString()
        {
            var ___dirname=Directory.GetCurrentDirectory();
            System.Console.WriteLine(___dirname);
            return @"Data Source="+___dirname+@"\northwind.db";
        }
    }

}