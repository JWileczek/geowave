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
package mil.nga.giat.geowave.adapter.vector.stats;

import java.nio.ByteBuffer;
import java.util.Date;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.FixedBinNumericStatistics;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 *
 * Fixed number of bins for a histogram. Unless configured, the range will
 * expand dynamically, redistributing the data as necessary into the wider bins.
 *
 * The advantage of constraining the range of the statistic is to ignore values
 * outside the range, such as erroneous values. Erroneous values force extremes
 * in the histogram. For example, if the expected range of values falls between
 * 0 and 1 and a value of 10000 occurs, then a single bin contains the entire
 * population between 0 and 1, a single bin represents the single value of
 * 10000. If there are extremes in the data, then use
 * {@link FeatureNumericHistogramStatistics} instead.
 *
 *
 * The default number of bins is 32.
 *
 */
public class FeatureFixedBinNumericStatistics extends
		FixedBinNumericStatistics<SimpleFeature> implements
		FeatureStatistic
{

	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FEATURE_FIXED_BIN_NUMERIC_HISTOGRAM");

	public FeatureFixedBinNumericStatistics() {
		super();
	}

	public FeatureFixedBinNumericStatistics(
			final String fieldName ) {
		this(
				null,
				fieldName);
	}

	public FeatureFixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final String fieldName ) {
		super(
				internalDataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldName));
	}

	public FeatureFixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final String fieldName,
			final int bins ) {
		super(
				internalDataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldName),
				bins);
	}

	public FeatureFixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final String fieldName,
			final int bins,
			final double minValue,
			final double maxValue ) {
		super(
				internalDataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldName),
				bins,
				minValue,
				maxValue);
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE.getString(),
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureFixedBinNumericStatistics(
				internalDataAdapterId,
				getFieldName());
	}

	@Override
	public void entryIngested(
			final SimpleFeature entry,
			final GeoWaveRow... rows ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		if (o instanceof Date) {
			add(
					1,
					((Date) o).getTime());
		}
		else if (o instanceof Number) {
			add(
					1,
					((Number) o).doubleValue());
		}
	}

	@Override
	public String getFieldIdentifier() {
		return getFieldName();
	}

	public static class FeatureFixedBinConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;
		private double minValue = Double.MAX_VALUE;
		private double maxValue = Double.MIN_VALUE;
		private int bins;

		public FeatureFixedBinConfig() {

		}

		public FeatureFixedBinConfig(
				final double minValue,
				final double maxValue,
				final int bins ) {
			super();
			this.minValue = minValue;
			this.maxValue = maxValue;
			this.bins = bins;
		}

		public double getMinValue() {
			return minValue;
		}

		public void setMinValue(
				final double minValue ) {
			this.minValue = minValue;
		}

		public double getMaxValue() {
			return maxValue;
		}

		public void setMaxValue(
				final double maxValue ) {
			this.maxValue = maxValue;
		}

		public int getBins() {
			return bins;
		}

		public void setBins(
				final int bins ) {
			this.bins = bins;
		}

		@Override
		public DataStatistics<SimpleFeature> create(
				final Short internalDataAdapterId,
				final String fieldName ) {
			return new FeatureFixedBinNumericStatistics(
					internalDataAdapterId,
					fieldName,
					bins,
					minValue,
					maxValue);
		}

		@Override
		public byte[] toBinary() {
			ByteBuffer buf = ByteBuffer.allocate(16);
			buf.putDouble(minValue);
			buf.putDouble(maxValue);
			return buf.array();
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			minValue = buf.getDouble();
			maxValue = buf.getDouble();
		}
	}
}
