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
package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.cli.exceptions.DuplicateEntryException;

@GeowaveOperation(name = "addws", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add GeoServer workspace")
public class GeoServerAddWorkspaceCommand extends
		GeoServerCommand<String>
{
	@Parameter(description = "<workspace name>")
	private List<String> parameters = new ArrayList<String>();
	private String wsName = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <workspace name>");
		}

		wsName = parameters.get(0);

		final Response addWorkspaceResponse = geoserverClient.addWorkspace(wsName);
		if (addWorkspaceResponse.getStatus() == Status.CREATED.getStatusCode()) {
			return "Add workspace '" + wsName + "' to GeoServer: OK";
		}
		String errorMessage = "Error adding workspace '" + wsName + "' to GeoServer: "
				+ addWorkspaceResponse.readEntity(String.class) + "\nGeoServer Response Code = "
				+ addWorkspaceResponse.getStatus();
		return handleError(
				addWorkspaceResponse,
				errorMessage);
	}
}
