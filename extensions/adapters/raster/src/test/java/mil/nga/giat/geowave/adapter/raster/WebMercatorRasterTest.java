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
package mil.nga.giat.geowave.adapter.raster;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

public class WebMercatorRasterTest
{
	public static final String CRS_STR = "EPSG:3857";

	@Test
	public void testStoreRetrieve()
			throws MismatchedIndexToAdapterMapping,
			IOException,
			MismatchedDimensionException,
			NoSuchAuthorityCodeException,
			FactoryException {

		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		DataStore dataStore = GeoWaveStoreFinder.createDataStore(Collections.EMPTY_MAP);
		int xTiles = 8;
		int yTiles = 8;
		double[] minsPerBand = new double[] {
			0,
			0,
			0
		};
		double[] maxesPerBand = new double[] {
			xTiles * 3 + yTiles * 24,
			xTiles * 3 + yTiles * 24,
			xTiles * 3 + yTiles * 24
		};
		String[] namesPerBand = new String[] {
			"b1",
			"b2",
			"b3"
		};
		RasterDataAdapter adapter = RasterUtils.createDataAdapterTypeDouble(
				"test",
				3,
				64,
				minsPerBand,
				maxesPerBand,
				namesPerBand,
				new NoDataMergeStrategy());
		PrimaryIndex index = new SpatialIndexBuilder().setCrs(
				CRS_STR) // 3857
				.createIndex();
		adapter.init(index);
		double bounds = CRS.decode(
				CRS_STR).getCoordinateSystem().getAxis(
				0).getMaximumValue();
		if (!Double.isFinite(bounds)) {
			bounds = SpatialDimensionalityTypeProvider.DEFAULT_UNBOUNDED_CRS_INTERVAL;
		}
		bounds /= 32.0;
		for (double xTile = 0; xTile < xTiles; xTile++) {
			for (double yTile = 0; yTile < yTiles; yTile++) {
				try (IndexWriter<GridCoverage> writer = dataStore.createWriter(
						adapter,
						index)) {
					final WritableRaster raster = RasterUtils.createRasterTypeDouble(
							3,
							64);
					RasterUtils.fillWithNoDataValues(
							raster,
							new double[][] {
								{
									xTile * 3 + yTile * 24
								},
								{
									xTile * 3 + yTile * 24 + 1
								},
								{
									xTile * 3 + yTile * 24 + 2
								}
							});
					writer.write(RasterUtils.createCoverageTypeDouble(
							"test",
							xTile * bounds,
							(xTile + 1) * bounds,
							yTile * bounds,
							(yTile + 1) * bounds,
							minsPerBand,
							maxesPerBand,
							namesPerBand,
							raster,
							CRS_STR));
				}
			}
		}
		int grid[][] = new int[8][8];
		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.createConfig(
						Collections.EMPTY_MAP,
						""));
		for (int xTile = 1; xTile < xTiles; xTile++) {
			for (int yTile = 1; yTile < yTiles; yTile++) {
				final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
						new double[] {
							// this is exactly on a tile boundary, so there
							// will be no
							// scaling on the tile composition/rendering

							(xTile - 15 / 64.0) * bounds,
							(yTile - 15 / 64.0) * bounds
						},
						new double[] {
							// these values are also on a tile boundary, to
							// avoid
							// scaling
							(xTile + 15 / 64.0) * bounds,
							(yTile + 15 / 64.0) * bounds
						});
				queryEnvelope.setCoordinateReferenceSystem(CRS.decode(CRS_STR));
				final GridCoverage gridCoverage = reader.renderGridCoverage(
						"test",
						new Rectangle(
								32,
								32),
						queryEnvelope,
						null,
						null,
						null);
				Raster img = gridCoverage.getRenderedImage().getData();

				grid[xTile - 1][yTile - 1] = img.getSample(
						0,
						16,
						0);
				grid[xTile - 1][yTile] = img.getSample(
						0,
						0,
						0);
				grid[xTile][yTile - 1] = img.getSample(
						16,
						16,
						0);
				grid[xTile][yTile] = img.getSample(
						16,
						0,
						0);

				double expectedMinXMinYValue = (xTile - 1) * 3 + (yTile - 1) * 24;
				double expectedMinXMaxYValue = (xTile - 1) * 3 + yTile * 24;
				double expectedMaxXMinYValue = xTile * 3 + (yTile - 1) * 24;
				double expectedMaxXMaxYValue = xTile * 3 + yTile * 24;
				for (int x = 0; x < 32; x++) {
					for (int y = 0; y < 32; y++) {

						for (int b = 0; b < 3; b++) {
							double expectedValue;
							if (x > 15) {
								if (y <= 15) {
									expectedValue = expectedMaxXMaxYValue;
								}
								else {
									expectedValue = expectedMaxXMinYValue;
								}
							}
							else if (y <= 15) {
								expectedValue = expectedMinXMaxYValue;
							}
							else {
								expectedValue = expectedMinXMinYValue;
							}
							expectedValue += b;

							Assert.assertEquals(
									String.format(
											"Value didn't match expected at x=%d;y=%d;b=%d",
											x,
											y,
											b),
									expectedValue,
									img.getSample(
											x,
											y,
											b),
									FloatCompareUtils.COMP_EPSILON);
						}
					}
				}

			}
		}
	}
}
