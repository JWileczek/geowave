package mil.nga.giat.geowave.analytic.spark;

import com.beust.jcommander.ParametersDelegate;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

/**
 * Created by jwileczek on 6/17/18.
 */
public abstract class SparkCommand extends
		ServiceEnabledCommand<Void>
{

	@ParametersDelegate
	private SparkJobParameters jobParameters = null;
	@ParametersDelegate
	private SparkApplicationOptions appOptions = new SparkApplicationOptions();

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {

		computeResults(params);
	}

	public SparkCommand() {}

	public SparkApplicationOptions getAppOptions() {
		return appOptions;
	}

	public void setAppOptions(
			SparkApplicationOptions appOptions ) {
		this.appOptions = appOptions;
	}
}
