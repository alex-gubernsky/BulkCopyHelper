using System;
using System.Data;
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
			if (options.CopyFrom > options.CopyTo)
			{
				Console.WriteLine("ERROR: Start Id is greater than end Id");
				return;
			}
				

			long minId, maxId;
			GetExistingIdRange(out minId, out maxId);

			long partitionSize = options.PartitionSize;
			long startId = CalculateStartId(options.CopyFrom, maxId);
			long endId = options.CopyTo;

			if (partitionSize != 0)
				endId = CalculateEndIdFromPartition(startId, partitionSize, options.CopyTo);

			if (Overlapps(minId, maxId, startId, endId))
			{
				Console.WriteLine($"ERROR: Specified Id range overlapps with existing records. MIN Id = {minId}, MAX Id = {maxId}");
				return;
			}

			int partitionNumber = 1;
			long lastCopiedId;
			while (CopyPartition(startId, endId, partitionNumber, options.UseTabLock, out lastCopiedId))
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

		private bool Overlapps(long minId, long maxId, long startId, long endId)
		{
			return Math.Max(minId, startId) <= Math.Min(maxId, endId);
		}

		private long CalculateStartId(long copyFrom, long maxId)
		{
			if (copyFrom > 0)
				return copyFrom;

			return maxId + 1;
		}

		private void GetExistingIdRange(out long minId, out long maxId)
		{
			minId = 0;
			maxId = 0;

			SqlConnection connection = new SqlConnection(_connectionString);
			SqlCommand getLastIdCommand = new SqlCommand(String.Format($"SELECT MIN(Id), MAX(Id) FROM TestRuns WHERE Id < {MaxId + 1}"), connection);

			IDataReader reader = null;
			try
			{
				connection.Open();
				reader = getLastIdCommand.ExecuteReader();
				if (reader.Read())
				{
					var minValue = reader.GetValue(0);
					if (minValue != null && minValue != DBNull.Value)
						minId = Convert.ToInt64(minValue);

					var maxValue = reader.GetValue(1);
					if (maxValue != null && maxValue != DBNull.Value)
						maxId = Convert.ToInt64(maxValue);
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
			finally
			{
				reader?.Close();
				connection.Close();
				connection.Dispose();
			}
		}

		private static long CalculateEndIdFromPartition(long startId, long partitionSize, long maxId)
		{
			long endId = startId + partitionSize - 1;
			if (endId > maxId)
				endId = maxId;
			return endId;
		}

		private bool CopyPartition(long startId, long endId, int partitionNumber, bool useTablock, out long lastCopiedId)
		{
			lastCopiedId = 0;
			SqlConnection connection = null;
			try
			{
				connection = new SqlConnection(_connectionString);

				connection.Open();

				SqlCommand getLastIdCommand = new SqlCommand(String.Format($"SELECT MAX(Id) FROM TestRuns WHERE Id < {MaxId + 1}"), connection);
				getLastIdCommand.CommandTimeout = 60;

				SqlCommand commandSourceData = new SqlCommand(String.Format($"SELECT * FROM TestRuns_Old WHERE Id >= {startId} AND Id <= {endId} ORDER BY Id ASC"), connection);
				commandSourceData.CommandTimeout = 60;

				SqlDataReader reader = commandSourceData.ExecuteReader();
				if (!reader.HasRows)
				{
					Console.WriteLine("No more rows to copy");
					return false;
				}

				Console.WriteLine($"Starting partition #{partitionNumber} from {startId} to {endId}:");

				SqlBulkCopyOptions options = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction;
				if (useTablock)
					options |= SqlBulkCopyOptions.TableLock;

				using (System.Data.SqlClient.SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connectionString, options))
				{
					bulkCopy.DestinationTableName = "dbo.TestRuns";
					bulkCopy.SqlRowsCopied += OnSqlRowsCopied;
					bulkCopy.NotifyAfter = 10000;
					bulkCopy.BatchSize = 5000;
					bulkCopy.BulkCopyTimeout = 0;
					bulkCopy.EnableStreaming = true;

					bulkCopy.ColumnMappings.Add("Id", "Id"); 
					bulkCopy.ColumnMappings.Add("Time", "Time");
					bulkCopy.ColumnMappings.Add("State", "State");
					bulkCopy.ColumnMappings.Add("Message", "Message");
					bulkCopy.ColumnMappings.Add("CallStack", "CallStack");
					bulkCopy.ColumnMappings.Add("Reason", "Reason");
					bulkCopy.ColumnMappings.Add("SuiteRunId", "SuiteRunId");
					bulkCopy.ColumnMappings.Add("TestId", "TestId");

					try
					{
						bulkCopy.WriteToServer(reader);
					}
					catch (Exception e)
					{
						Console.WriteLine($"\n{e}");
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
							Console.WriteLine($"\n{e}");
							throw;
						}
					}
				}

				return true;
			}
			catch (Exception e)
			{
				Console.WriteLine($"\n{e}");
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