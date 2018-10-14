package mil.nga.giat.geowave.analytic.spark;

import com.beust.jcommander.Parameter;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;

import java.util.Map;

/**
 * Created by jwileczek on 6/17/18.
 */
public class SparkStoreOptions
{

	public SparkStoreOptions(
			final String storeName ) {
		this.storeName = storeName;
	}

	private String storeName;

	public DataStorePluginOptions getStorePlugin() {
		ConfigOptions.getDefaultPropertyFile();
		return null;
	}
}
