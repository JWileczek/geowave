/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.EntryIteratorWrapper;

public class HBaseEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseEntryIteratorWrapper.class);
	private boolean decodePersistenceEncoding = true;

	private byte[] fieldSubsetBitmask;

	private Integer bitPosition;
	private ByteArrayId skipUntilRow;

	private boolean reachedEnd = false;
	private boolean hasSkippingFilter = false;

	public HBaseEntryIteratorWrapper(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ?> scanCallback,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding,
			final boolean hasSkippingFilter ) {
		super(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
		this.decodePersistenceEncoding = decodePersistenceEncoding;
		this.hasSkippingFilter = hasSkippingFilter;

		if (!this.hasSkippingFilter) {
			initializeBitPosition(maxResolutionSubsamplingPerDimension);
		}
		else {
			bitPosition = null;
		}

		if (fieldIds != null) {
			fieldSubsetBitmask = BitmaskUtils.generateFieldSubsetBitmask(
					index.getIndexModel(),
					ByteArrayId.transformStringList(fieldIds.getLeft()),
					fieldIds.getRight());
		}
	}

	@Override
	protected T decodeRow(
			GeoWaveRow row,
			QueryFilter clientFilter,
			PrimaryIndex index ) {
		Result result = null;
		try {
			// TODO: construct GeoWaveRow from Result
			result = null;
		}
		catch (final ClassCastException e) {
			LOGGER.error(
					"Row is not an HBase row Result.",
					e);
			return null;
		}

		if (passesResolutionSkippingFilter(result)) {
			return (T) HBaseUtils.decodeRow(
					result,
					row,
					adapterStore,
					clientFilter,
					index,
					false);
		}
		return null;
	}

	private boolean passesResolutionSkippingFilter(
			final Result result ) {
		if (hasSkippingFilter) {
			return true;
		}

		if ((reachedEnd == true) || ((skipUntilRow != null) && (skipUntilRow.compareTo(new ByteArrayId(
				result.getRow())) > 0))) {
			return false;
		}
		incrementSkipRow(result);
		return true;
	}

	private void incrementSkipRow(
			final Result result ) {
		if (bitPosition != null) {
			final byte[] nextRow = IndexUtils.getNextRowForSkip(
					result.getRow(),
					bitPosition);

			if (nextRow == null) {
				reachedEnd = true;
			}
			else {
				skipUntilRow = new ByteArrayId(
						nextRow);
			}
		}
	}

	private void initializeBitPosition(
			final double[] maxResolutionSubsamplingPerDimension ) {
		if ((maxResolutionSubsamplingPerDimension != null) && (maxResolutionSubsamplingPerDimension.length > 0)) {
			bitPosition = IndexUtils.getBitPositionFromSubsamplingArray(
					index.getIndexStrategy(),
					maxResolutionSubsamplingPerDimension);
		}
	}
}
