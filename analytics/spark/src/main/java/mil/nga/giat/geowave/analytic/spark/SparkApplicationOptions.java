package mil.nga.giat.geowave.analytic.spark;

import com.beust.jcommander.Parameter;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;

import java.nio.ByteBuffer;

/**
 * Created by jwileczek on 6/17/18.
 */
public class SparkApplicationOptions implements
		Persistable
{

	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName = "SparkApp";

	@Parameter(names = {
		"-ho",
		"--host"
	}, description = "The spark driver host")
	private String host = "localhost";

	@Parameter(names = {
		"-m",
		"--master"
	}, description = "The spark master designation")
	private String master = "yarn";

	public SparkApplicationOptions() {}

	public SparkApplicationOptions(
			final String defaultAppName ) {
		this.appName = defaultAppName;
	}

	public SparkApplicationOptions(
			final String defaultAppName,
			final String defaultHost,
			final String defaultMaster ) {
		this.appName = defaultAppName;
		this.host = defaultHost;
		this.master = defaultMaster;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(
			String appName ) {
		this.appName = appName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(
			String host ) {
		this.host = host;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(
			String master ) {
		this.master = master;
	}

	@Override
	public byte[] toBinary() {
		final byte[] appNameBytes = ByteArrayUtils.byteArrayFromString(this.appName);
		final byte[] hostBytes = ByteArrayUtils.byteArrayFromString(this.host);
		final byte[] masterBytes = ByteArrayUtils.byteArrayFromString(this.master);
		final int appNameSize = appNameBytes.length;
		final int hostSize = hostBytes.length;
		final int masterSize = masterBytes.length;
		final byte[] resultBuffer = new byte[appNameSize + hostSize + masterSize + (Integer.BYTES * 3)];

		final ByteBuffer buf = ByteBuffer.wrap(resultBuffer);
		buf.putInt(appNameSize);
		buf.put(appNameBytes);

		buf.putInt(hostSize);
		buf.put(hostBytes);

		buf.putInt(masterSize);
		buf.put(masterBytes);

		return resultBuffer;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int nameSize = buf.getInt();
		final byte[] nameBytes = new byte[nameSize];
		buf.get(nameBytes);
		final int hostSize = buf.getInt();
		final byte[] hostBytes = new byte[hostSize];
		buf.get(hostBytes);

		final int masterSize = buf.getInt();
		final byte[] masterBytes = new byte[masterSize];
		buf.get(masterBytes);

		this.appName = ByteArrayUtils.byteArrayToString(nameBytes);
		this.host = ByteArrayUtils.byteArrayToString(hostBytes);
		this.master = ByteArrayUtils.byteArrayToString(masterBytes);
	}
}
