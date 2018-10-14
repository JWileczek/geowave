package mil.nga.giat.geowave.analytic.spark.typed;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class IndexedGeometry
{
	public IndexedGeometry(
			ByteArrayId insertID,
			GeoWaveInputKey inputKey,
			Geometry geom ) {
		this.inputKey = inputKey;
		this.insertionID = insertID;
		this.geom = geom;
	}

	public ByteArrayId insertionID;
	public GeoWaveInputKey inputKey;
	public Geometry geom;
}
