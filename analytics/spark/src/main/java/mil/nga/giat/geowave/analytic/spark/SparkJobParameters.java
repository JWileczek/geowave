package mil.nga.giat.geowave.analytic.spark;

import mil.nga.giat.geowave.core.index.persist.Persistable;

/**
 * Created by jwileczek on 7/18/18.
 */
public interface SparkJobParameters extends
		Persistable
{

	public boolean checkRequiredParameters();
}
