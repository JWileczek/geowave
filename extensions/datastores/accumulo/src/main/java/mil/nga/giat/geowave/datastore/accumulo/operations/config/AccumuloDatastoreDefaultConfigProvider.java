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
package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import java.util.Properties;

import mil.nga.giat.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class AccumuloDatastoreDefaultConfigProvider implements
		DefaultConfigProviderSpi
{
	private Properties configProperties = new Properties();

	/**
	 * Create the properties for the config-properties file
	 */
	private void setProperties() {
		configProperties.setProperty(
				"store.default-accumulo.opts.createTable",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.enableBlockCache",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.gwNamespace",
				"geowave.default");
		configProperties.setProperty(
				"store.default-accumulo.opts.instance",
				"accumulo");
		configProperties.setProperty(
				"store.default-accumulo.opts.password",
				"secret");
		configProperties.setProperty(
				"store.default-accumulo.opts.persistAdapter",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.persistDataStatistics",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.persistIndex",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.useAltIndex",
				"false");
		configProperties.setProperty(
				"store.default-accumulo.opts.useLocalityGroups",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.user",
				"root");
		configProperties.setProperty(
				"store.default-accumulo.opts.zookeeper",
				"localhost:2181");
		configProperties.setProperty(
				"store.default-accumulo.type",
				"accumulo");
	}

	@Override
	public Properties getDefaultConfig() {
		setProperties();
		return configProperties;
	}

}
