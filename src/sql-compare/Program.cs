using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;

using Shouldly;

namespace sql_compare
{
    class Program
    {
        #region Methods

        #region Private Static Methods

        private static void CompareResultSet(SqlDataReader originalDataReader, SqlDataReader compareDataReader, int resultSetIndex = 1)
        {
            originalDataReader.FieldCount.ShouldBe(compareDataReader.FieldCount,
                $"Original fields: {string.Join(", ", GetFieldNames(originalDataReader))}. Compared fields: {string.Join(", ", GetFieldNames(compareDataReader))}.");

            for (int i = 0; i < originalDataReader.FieldCount; i++)
            {
                originalDataReader.GetName(i).ShouldBe(compareDataReader.GetName(i), $"Result set {resultSetIndex}");
            }

            while (Read(originalDataReader, compareDataReader))
            {
                for (int i = 0; i < originalDataReader.FieldCount; i++)
                {
                    originalDataReader[i].ShouldBe(compareDataReader[i], $"Field: {originalDataReader.GetName(i)}");
                }
            }

            var originalHasNextResult = originalDataReader.NextResult();
            var compareHasNextResult = compareDataReader.NextResult();

            if (originalHasNextResult && compareHasNextResult)
                CompareResultSet(originalDataReader, compareDataReader, resultSetIndex: resultSetIndex + 1);

            if (!originalHasNextResult && compareHasNextResult)
                throw new InvalidOperationException($"The compared file should not have a next result {resultSetIndex + 1}!");

            if (originalHasNextResult && !compareHasNextResult)
                throw new InvalidOperationException($"The compared file should have a next result {resultSetIndex + 1}!");
        }

        private static IEnumerable<string> GetFieldNames(SqlDataReader sqlDataReader)
        {
            for (int i = 0; i < sqlDataReader.FieldCount; i++)
            {
                yield return sqlDataReader.GetName(i);
            }
        }

        private static bool Read(SqlDataReader originalDataReader, SqlDataReader compareDataReader)
        {
            var originalHasNextRow = originalDataReader.Read();
            var compareHasNextRow = compareDataReader.Read();

            if (originalHasNextRow && compareHasNextRow)
                return true;

            originalHasNextRow.ShouldBe(compareHasNextRow);

            return false;
        }

        private static bool TryGetSettings(string[] args, out string original, out string compare, out string sqlConnectionString)
        {
            original = null;
            compare = null;
            sqlConnectionString = null;

            if (args.Length != 3)
                return false;

            original = File.Exists(args[0]) ? File.ReadAllText(args[0]) : null;
            compare = File.Exists(args[1]) ? File.ReadAllText(args[1]) : null;
            sqlConnectionString = args[2];

            return !string.IsNullOrWhiteSpace(original)
                && !string.IsNullOrWhiteSpace(compare)
                && !string.IsNullOrWhiteSpace(sqlConnectionString);
        }

        #endregion Private Static Methods

        #region Internal Methods

        internal static void Process(string original, string compare, string sqlConnectionString)
        {
            var builder = new SqlConnectionStringBuilder(sqlConnectionString);
            builder.MultipleActiveResultSets = true;

            using (var connection = new SqlConnection(builder.ConnectionString))
            {
                connection.StatisticsEnabled = true;
                connection.Open();

                using (var originalCommand = new SqlCommand(original, connection))
                using (var compareCommand = new SqlCommand(compare, connection))
                {
                    var originalDataReader = originalCommand.ExecuteReader();
                    var originalCommandStats = connection.RetrieveStatistics();
                    var originalCommandExecutionTimeInMs = (long)originalCommandStats["ExecutionTime"];

                    // reset for next command
                    connection.ResetStatistics();

                    var compareDataReader = compareCommand.ExecuteReader();
                    var compareCommandStats = connection.RetrieveStatistics();
                    var compareCommandExecutionTimeInMs = (long)compareCommandStats["ExecutionTime"];

                    Console.WriteLine("Time of original: {0}ms. Time of compared: {1}ms. The compared file is {2}.",
                        originalCommandExecutionTimeInMs,
                        compareCommandExecutionTimeInMs,
                        originalCommandExecutionTimeInMs > compareCommandExecutionTimeInMs ? "faster" : "slower");

                    CompareResultSet(originalDataReader, compareDataReader);
                }
            }

            Console.WriteLine("SUCCESS: The results of original and compared file are identically.");
        }

        #endregion Internal Methods

        #region Private Methods

        static int Main(string[] args)
        {
            try
            {
                if (!TryGetSettings(args, out var original, out var compare, out var sqlConnectionString))
                {
                    Console.WriteLine("Usage: sql-compare.exe myoriginal.sql mychanged.sql \"sql connection string\"");
                    return 1;
                }

                Process(original, compare, sqlConnectionString);

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                return 2;
            }
        }

        #endregion Private Methods

        #endregion Methods
    }
}