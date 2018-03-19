using System;
using System.Configuration;

namespace SqlBulkCopy
{
	class Program
	{
		static void Main(string[] args)
		{
			var connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
			Options options = ReadOptions(args);

			DateTime t = DateTime.Now;

			BulkCopyHelper helper = new BulkCopyHelper(connectionString);
			helper.Copy(options);

			var dt = DateTime.Now - t;
			Console.WriteLine("Completed in {0} seconds", dt.TotalSeconds);
		}

		private static Options ReadOptions(string[] args)
		{
			Options options = new Options();
			foreach (var s in args)
			{
				string[] paramStrings = s.Split('=');
				if (paramStrings.Length != 2)
					continue;

				switch (paramStrings[0].ToUpper())
				{
					case "COPY_FROM":
						options.CopyFrom = Int64.Parse(paramStrings[1]);
						break;
					case "COPY_TO":
						options.CopyTo = Int64.Parse(paramStrings[1]);
						break;
					case "PARTITION":
						options.PartitionSize = Int64.Parse(paramStrings[1]);
						break;
					case "SLEEP":
						options.Sleep = Int32.Parse(paramStrings[1]);
						break;
				}
			}

			return options;
		}
	}
}
