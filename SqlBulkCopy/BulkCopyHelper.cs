using System;
using System.Data.SqlClient;
using System.Threading;

namespace SqlBulkCopy
{
	public class BulkCopyHelper
	{
		private const long MaxId = Int32.MaxValue;
		private readonly string _connectionString;
		public BulkCopyHelper(string connectionString)
		{
			_connectionString = connectionString;
		}

		public void Copy(Options options)
		{
			long partitionSize = options.PartitionSize;
			long startId = options.CopyFrom;
			long endId = options.CopyTo;

			if (partitionSize != 0)
				endId = CalculateEndIdFromPartition(startId, partitionSize, options.CopyTo);

			int partitionNumber = 1;
			long lastCopiedId;
			while (CopyPartition(startId, endId, partitionNumber, out lastCopiedId))
			{
				startId = lastCopiedId + 1;
				endId = CalculateEndIdFromPartition(startId, partitionSize, options.CopyTo);

				partitionNumber++;

				if (options.Sleep > 0)
				{
					Console.WriteLine($"Sleeping for {options.Sleep} ms...");
					Thread.Sleep(options.Sleep);
					Console.WriteLine("Resuming work...");
				}
			}
		}

		private static long CalculateEndIdFromPartition(long startId, long partitionSize, long maxId)
		{
			long endId = startId + partitionSize - 1;
			if (endId > maxId)
				endId = maxId;
			return endId;
		}

		private bool CopyPartition(long startId, long endId, int partitionNumber, out long lastCopiedId)
		{
			lastCopiedId = 0;
			SqlConnection connection = null;
			try
			{
				connection = new SqlConnection(_connectionString);

				connection.Open();

				SqlCommand totalRowsCountCommand = new SqlCommand(String.Format($"SELECT COUNT(*) FROM TestRuns_Old WHERE Id >= {startId} AND Id <= {endId}"), connection);
				long totalRows = Convert.ToInt64(totalRowsCountCommand.ExecuteScalar());

				if (totalRows == 0)
				{
					Console.WriteLine("No more rows to copy");
					return false;
				}

				Console.WriteLine($"Starting partition #{partitionNumber} from {startId} to {endId}. Ready to copy {totalRows} rows:");

				SqlCommand getLastIdCommand = new SqlCommand(String.Format($"SELECT MAX(Id) FROM TestRuns WHERE Id < {MaxId + 1}"), connection);

				SqlCommand commandSourceData = new SqlCommand(String.Format($"SELECT * FROM TestRuns_Old WHERE Id >= {startId} AND Id <= {endId} ORDER BY Id ASC"), connection);
				SqlDataReader reader = commandSourceData.ExecuteReader();

				using (System.Data.SqlClient.SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connectionString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction))
				{
					bulkCopy.DestinationTableName = "dbo.TestRuns";
					bulkCopy.SqlRowsCopied += OnSqlRowsCopied;
					bulkCopy.NotifyAfter = 10000;
					bulkCopy.BatchSize = 5000;
					bulkCopy.BulkCopyTimeout = 0;
					bulkCopy.EnableStreaming = true;

					try
					{
						bulkCopy.WriteToServer(reader);
					}
					catch (Exception ex)
					{
						Console.WriteLine(ex);
						throw;
					}
					finally
					{
						reader.Close();

						try
						{
							lastCopiedId = Convert.ToInt64(getLastIdCommand.ExecuteScalar());
							Console.WriteLine($"\n\tPartition #{partitionNumber} completed. Last Id in this partition = {lastCopiedId}");
						}
						catch (Exception e)
						{
							Console.WriteLine(e);
							throw;
						}
					}
				}

				return true;
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
			finally
			{
				connection?.Close();
				connection?.Dispose();
			}
		}

		private static void OnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			Console.Write($"\r\tRecords copied: {e.RowsCopied}");
		}
	}
}