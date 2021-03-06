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
package mil.nga.giat.geowave.datastore.cassandra.util;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class SessionPool
{

	private static SessionPool singletonInstance;

	public static synchronized SessionPool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new SessionPool();
		}
		return singletonInstance;
	}

	protected SessionPool() {}

	private final Map<String, Session> sessionCache = new HashMap<String, Session>();

	public synchronized Session getSession(
			final String contactPoints ) {
		Session session = sessionCache.get(contactPoints);
		if (session == null) {
			session = Cluster.builder().addContactPoints(
					contactPoints.split(",")).build().connect();
			sessionCache.put(
					contactPoints,
					session);
		}
		return session;
	}
}
