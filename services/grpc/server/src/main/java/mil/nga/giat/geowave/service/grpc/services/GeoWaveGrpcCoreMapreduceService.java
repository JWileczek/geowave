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
package mil.nga.giat.geowave.service.grpc.services;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import mil.nga.giat.geowave.service.grpc.protobuf.CoreMapreduceGrpc.CoreMapreduceImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcCoreMapreduceService extends
		CoreMapreduceImplBase implements
		GeoWaveGrpcServiceSpi
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreMapreduceService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void configHDFSCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ConfigHDFSCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		ConfigHDFSCommand cmd = new ConfigHDFSCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing ConfigHDFSCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}
}
