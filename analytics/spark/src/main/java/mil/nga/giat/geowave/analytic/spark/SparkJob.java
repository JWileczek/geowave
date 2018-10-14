package mil.nga.giat.geowave.analytic.spark;

import com.beust.jcommander.ParameterException;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jwileczek on 7/14/18.
 */
public abstract class SparkJob implements
		Persistable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(SparkJob.class);

	private SparkApplicationOptions appOptions = null;
	private SparkJobParameters jobParameters = null;

	private SparkSession session = null;

	public abstract boolean initParams();

	public abstract void computeResults();

	public boolean initJob() {

		return initSession() && initParams();
	}

	public boolean initSession() {
		if (session == null) {
			if (this.appOptions == null) {
				LOGGER
						.error("Spark Application options not set to create SparkSession for job. Please set application options and run again.");
				return false;
			}

			session = GeoWaveSparkConf.createSessionFromParams(
					this.appOptions.getAppName(),
					this.appOptions.getMaster(),
					this.appOptions.getHost(),
					null);
		}
		return true;
	}

	public void run() {
		if (!initJob()) {
			LOGGER.error("Failed to initialize SparkJob check logs for more information.");
			return;
		}

		computeResults();
	}

	public SparkSession getSession() {
		return session;
	}

	public void setSession(
			SparkSession session ) {
		this.session = session;
	}

	public SparkApplicationOptions getAppOptions() {
		return appOptions;
	}

	public void setAppOptions(
			SparkApplicationOptions appOptions ) {
		this.appOptions = appOptions;
	}

	public SparkJobParameters getJobParameters() {
		return jobParameters;
	}

	public void setJobParameters(
			SparkJobParameters jobParameters ) {
		this.jobParameters = jobParameters;
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {

	}
}
