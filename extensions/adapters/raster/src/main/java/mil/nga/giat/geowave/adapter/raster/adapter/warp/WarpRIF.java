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
/* JAI-Ext - OpenSource Java Advanced Image Extensions Library
 *    http://www.geo-solutions.it/
 *    Copyright 2014 GeoSolutions
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mil.nga.giat.geowave.adapter.raster.adapter.warp;

import java.awt.RenderingHints;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.media.jai.OperationRegistry;
import javax.media.jai.PlanarImage;
import javax.media.jai.ROI;
import javax.media.jai.Warp;
import javax.media.jai.operator.MosaicDescriptor;
import javax.media.jai.registry.RenderedRegistryMode;

import com.sun.media.jai.opimage.MosaicRIF;
import com.sun.media.jai.opimage.RIFUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import it.geosolutions.jaiext.interpolators.InterpolationNearest;
import it.geosolutions.jaiext.range.Range;
import it.geosolutions.jaiext.range.RangeFactory;
import mil.nga.giat.geowave.adapter.raster.adapter.SourceThresholdFixMosaicDescriptor;

/**
 * 
 * This is code entirely intended to get around an issue on line 265 of
 * WarpOpImage in jai-ext. The following code does not work if the source is
 * significant lower resolution than the destination and seems unnecessary in
 * general:
 * 
 * roiTile = roi.intersect(new ROIShape(srcRectExpanded));
 *
 */
@SuppressFBWarnings
public class WarpRIF extends
		it.geosolutions.jaiext.warp.WarpRIF
{
	static boolean registered = false;

	public synchronized static void register(
			final boolean force ) {
		if (!registered || force) {
			final OperationRegistry registry = JAI.getDefaultInstance().getOperationRegistry();

			final RenderedImageFactory rif = new WarpRIF();
			registry.registerFactory(
					RenderedRegistryMode.MODE_NAME,
					"Warp",
					"it.geosolutions.jaiext",
					rif);
			registered = true;
		}
	}

	/** Constructor. */
	public WarpRIF() {}

	/**
	 * Creates a new instance of warp operator according to the warp object and
	 * interpolation method.
	 * 
	 * @param paramBlock
	 *            The warp and interpolation objects.
	 */
	public RenderedImage create(
			ParameterBlock paramBlock,
			RenderingHints renderHints ) {
		Interpolation interp = (Interpolation) paramBlock.getObjectParameter(1);
		if (interp instanceof InterpolationNearest || interp instanceof javax.media.jai.InterpolationNearest) {
			// Get ImageLayout from renderHints if any.
			ImageLayout layout = RIFUtil.getImageLayoutHint(renderHints);

			RenderedImage source = paramBlock.getRenderedSource(0);
			Warp warp = (Warp) paramBlock.getObjectParameter(0);
			double[] backgroundValues = (double[]) paramBlock.getObjectParameter(2);

			ROI roi = null;
			Object roi_ = paramBlock.getObjectParameter(3);
			if (roi_ instanceof ROI) {
				roi = (ROI) roi_;
				PlanarImage temp = PlanarImage.wrapRenderedImage(source);
				temp.setProperty(
						"ROI",
						roi);
				source = temp;
			}
			Range noData = (Range) paramBlock.getObjectParameter(4);
			noData = RangeFactory.convert(
					noData,
					source.getSampleModel().getDataType());
			return new WarpNearestOpImage(
					source,
					renderHints,
					layout,
					warp,
					interp,
					roi,
					noData,
					backgroundValues);
		}
		return super.create(
				paramBlock,
				renderHints);
	}
}
