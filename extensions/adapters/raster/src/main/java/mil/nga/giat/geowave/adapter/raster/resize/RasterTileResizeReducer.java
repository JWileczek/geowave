/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, GridCoverage>
{
	private RasterTileResizeHelper helper;

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		final GridCoverage mergedCoverage = helper.getMergedCoverage(
				key,
				values);
		if (mergedCoverage != null) {
			context.write(
					helper.getGeoWaveOutputKey(),
					mergedCoverage);
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
