using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;
using System.Configuration;

namespace MySQL2MSSQL_Data_Migration_Utility
{
    internal class Program
    {
        static void Main(string[] args)
        {
            MigrateData();
        }

        static void MigrateData()
        {
            string mysqlConnString = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;
            string mssqlConnString = ConfigurationManager.ConnectionStrings["mssql"].ConnectionString;

            string mysqlQuery = ConfigurationManager.AppSettings["mysqlQuery"];
            string mssqlQuery = ConfigurationManager.AppSettings["mssqlQuery"];

            try
            {
                using (OdbcConnection mysqlConn = new OdbcConnection(mysqlConnString))
                using (SqlConnection mssqlConn = new SqlConnection(mssqlConnString))
                {
                    mysqlConn.Open();
                    mssqlConn.Open();

                    using (OdbcCommand mysqlCmd = new OdbcCommand(mysqlQuery, mysqlConn))
                    using (SqlCommand mssqlCmd = new SqlCommand(mssqlQuery, mssqlConn))
                    {
                        using (OdbcDataReader mysqlReader = mysqlCmd.ExecuteReader())
                        {
                            while (mysqlReader.Read())
                            {
                                mssqlCmd.Parameters.Clear();

                                for (int i = 0; i < 1000 && mysqlReader.Read(); i++)
                                {
                                    for (int j = 0; j < mysqlReader.FieldCount; j++)
                                    {
                                        mssqlCmd.Parameters.AddWithValue($"@value{i + 1}_{j + 1}", mysqlReader[j]);
                                    }
                                }

                                mssqlCmd.ExecuteNonQuery();
                            }
                        }
                    }
                }
                Console.WriteLine("Данните са преместени успешно.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Има грешка при преместването на данните: {ex.Message}");
            }
        }
    }
}
