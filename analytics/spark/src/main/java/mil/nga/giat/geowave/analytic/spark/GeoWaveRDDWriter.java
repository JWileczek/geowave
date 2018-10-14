package mil.nga.giat.geowave.analytic.spark;

import com.google.common.collect.Lists;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

public class GeoWaveRDDWriter
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRDDWriter.class);

	// Write RDD to output store using existing feature adapter using index
	// strategies set for adapter
	public static void writeRDD(
			final SparkContext sc,
			final DataStorePluginOptions outputStore,
			final ByteArrayId outputAdapterId,
			final GeoWaveRDD rdd )
			throws IOException {
		// Grab necessary stores
		AdapterStore adapterStore = outputStore.createAdapterStore();
		IndexStore indexStore = outputStore.createIndexStore();
		short internalId = outputStore.createInternalAdapterStore().getInternalAdapterId(
				outputAdapterId);
		DataAdapter<?> adapterToWrite = adapterStore.getAdapter(outputAdapterId);
		if (adapterToWrite == null) {
			LOGGER.error("Adapter named " + outputAdapterId.getString() + " does not exist in store.");
			return;
		}

		PrimaryIndex[] indices = outputStore.createAdapterIndexMappingStore().getIndicesForAdapter(
				internalId).getIndices(
				indexStore);
		GeoWaveRDDWriter.writeRDD(
				sc,
				outputStore,
				adapterToWrite,
				indices,
				rdd);
	}

	// Write RDD to specific provided adapter using index strategies
	public static void writeRDD(
			final SparkContext sc,
			final DataStorePluginOptions outputStore,
			final DataAdapter outputAdapter,
			final PrimaryIndex[] outputIndices,
			final GeoWaveRDD rdd )
			throws IOException {

		if (outputIndices.length == 0) {
			LOGGER
					.error("No indexing strategies provided for write. At least one indexing strategy should be provided to output RDD");
			return;
		}
		JavaRDD<SimpleFeature> featuresToWrite = rdd.getRawRDD().values();
		short internalId = outputStore.createInternalAdapterStore().getInternalAdapterId(
				outputAdapter.getAdapterId());
		for (PrimaryIndex index : outputIndices) {
			GeoWaveRDDWriter.writeToGeoWave(
					sc,
					outputStore,
					outputAdapter,
					internalId,
					index,
					featuresToWrite);
		}
	}

	/**
	 * Translate a set of objects in a JavaRDD to a provided type and push to
	 * GeoWave
	 *
	 * @throws IOException
	 */
	private static void writeToGeoWave(SparkContext sc,
                                       DataStorePluginOptions outputStoreOptions,
                                       DataAdapter adapter,
									   short adapterId,
                                       PrimaryIndex index,
                                       JavaRDD<SimpleFeature> inputRDD) throws IOException{

        Broadcast<PrimaryIndex> broadcastedIndex = sc.broadcast(index, scala.reflect.ClassTag$.MODULE$.apply(PrimaryIndex.class));
        Broadcast<DataAdapter> broadcastedAdapter = sc.broadcast(adapter, scala.reflect.ClassTag$.MODULE$.apply(DataAdapter.class));

        // Transform and generate keys for each feature.
        JavaPairRDD<GeoWaveKey, SimpleFeature> writeRDD = inputRDD.flatMapToPair((PairFlatMapFunction<SimpleFeature, GeoWaveKey, SimpleFeature>) feat -> {
            AdapterPersistenceEncoding persistInfo = broadcastedAdapter.value().encode(feat, broadcastedIndex.value().getIndexModel());

            GeoWaveKey[] keys = GeoWaveKeyImpl.createKeys(
                    persistInfo.getInsertionIds(broadcastedIndex.value()),
                    persistInfo.getDataId().getBytes(),
					adapterId);

            ArrayList<Tuple2<GeoWaveKey,SimpleFeature>> results = Lists.newArrayList();
            for (GeoWaveKey key : keys) {
                results.add(new Tuple2<GeoWaveKey,SimpleFeature>(key,feat));
            }
            return results.iterator();
        });

        //setup the configuration and the output format
        Configuration conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration());

        GeoWaveOutputFormat.setStoreOptions(conf, outputStoreOptions);
        GeoWaveOutputFormat.addIndex(conf, index);
        GeoWaveOutputFormat.addDataAdapter(conf, adapter);
        //create the job
        Job job = new Job(conf);
        job.setOutputKeyClass(GeoWaveKey.class);
        job.setOutputValueClass(SimpleFeature.class);
        job.setOutputFormatClass(GeoWaveOutputFormat.class);

        //map to a pair containing the output key and the output value
        writeRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }
}
