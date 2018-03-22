using System;

namespace SqlBulkCopy
{
	public class Options
	{
		private long _copyFrom;
		private long _copyTo;
		public long CopyFrom
		{
			get
			{
				if (_copyFrom != 0)
					return _copyFrom;
				return 0;
			}
			set { _copyFrom = value; }
		}

		public long CopyTo
		{
			get
			{
				if (_copyTo != 0)
					return _copyTo;
				return Int32.MaxValue;
			}
			set { _copyTo = value; }
		}
		public long PartitionSize { get; set; }
		public int Sleep { get; set; }
		public bool UseTabLock { get; set; }
	}
}