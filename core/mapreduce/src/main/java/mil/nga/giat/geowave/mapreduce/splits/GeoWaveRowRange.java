package mil.nga.giat.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GeoWaveRowRange implements
		Writable
{
	private byte[] partitionKey;
	private byte[] startKey;
	private byte[] endKey;
	private boolean startKeyInclusive;
	private boolean endKeyInclusive;

	protected GeoWaveRowRange() {}

	public GeoWaveRowRange(
			final byte[] partitionKey,
			final byte[] startKey,
			final byte[] endKey,
			final boolean startKeyInclusive,
			final boolean endKeyInclusive ) {
		this.partitionKey = partitionKey;
		this.startKey = startKey;
		this.endKey = endKey;
		this.startKeyInclusive = startKeyInclusive;
		this.endKeyInclusive = endKeyInclusive;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeBoolean(
				(partitionKey == null) || (partitionKey.length == 0));
		out.writeBoolean(
				startKey == null);
		out.writeBoolean(
				endKey == null);
		if ((partitionKey != null) && (partitionKey.length > 0)) {
			out.writeShort(
					partitionKey.length);
			out.write(
					partitionKey);
		}
		if (startKey != null) {
			out.writeShort(
					startKey.length);
			out.write(
					startKey);
		}
		if (endKey != null) {
			out.writeShort(
					endKey.length);
			out.write(
					endKey);
		}
		out.writeBoolean(
				startKeyInclusive);
		out.writeBoolean(
				endKeyInclusive);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final boolean nullPartitionKey = in.readBoolean();
		final boolean infiniteStartKey = in.readBoolean();
		final boolean infiniteEndKey = in.readBoolean();
		if (!nullPartitionKey) {
			partitionKey = new byte[in.readShort()];
			in.readFully(
					partitionKey);
		}
		if (!infiniteStartKey) {
			startKey = new byte[in.readShort()];
			in.readFully(
					startKey);
		}
		else {
			startKey = null;
		}

		if (!infiniteEndKey) {
			endKey = new byte[in.readShort()];
			in.readFully(
					endKey);
		}
		else {
			endKey = null;
		}

		startKeyInclusive = in.readBoolean();
		endKeyInclusive = in.readBoolean();
	}

	public byte[] getPartitionKey() {
		return partitionKey;
	}

	public byte[] getStartSortKey() {
		return startKey;
	}

	public byte[] getEndSortKey() {
		return endKey;
	}

	public boolean isStartSortKeyInclusive() {
		return startKeyInclusive;
	}

	public boolean isEndSortKeyInclusive() {
		return endKeyInclusive;
	}

	public boolean isInfiniteStartSortKey() {
		return startKey == null;
	}

	public boolean isInfiniteStopSortKey() {
		return endKey == null;
	}
}
