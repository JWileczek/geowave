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
package mil.nga.giat.geowave.core.index.simple;

import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;

/**
 * A simple 1-dimensional NumericIndexStrategy that represents an index of
 * signed long values. The strategy doesn't use any binning. The ids are simply
 * the byte arrays of the value. This index strategy will not perform well for
 * inserting ranges because there will be too much replication of data.
 *
 */
public class SimpleLongIndexStrategy extends
		SimpleNumericIndexStrategy<Long>
{

	public SimpleLongIndexStrategy() {
		super(
				Lexicoders.LONG);
	}

	@Override
	protected Long cast(
			final double value ) {
		return (long) value;
	}

}
