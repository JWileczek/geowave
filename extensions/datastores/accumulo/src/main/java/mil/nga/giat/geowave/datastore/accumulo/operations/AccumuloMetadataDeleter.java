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
package mil.nga.giat.geowave.datastore.accumulo.operations;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

public class AccumuloMetadataDeleter implements
		MetadataDeleter
{

	private final AccumuloOperations operations;
	private final String metadataTypeName;

	public AccumuloMetadataDeleter(
			final AccumuloOperations operations,
			final MetadataType metadataType ) {
		super();
		this.operations = operations;
		metadataTypeName = metadataType.name();
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public boolean delete(
			final MetadataQuery query ) {
		// the nature of metadata deleter is that primary ID is always
		// well-defined and it is deleting a single entry at a time
		return operations.delete(
				AbstractGeoWavePersistence.METADATA_TABLE,
				new ByteArrayId(
						query.getPrimaryId()),
				metadataTypeName,
				query.getSecondaryId() != null ? query.getSecondaryId() : null,
				query.getAuthorizations());
	}

	@Override
	public void flush() {}

}
