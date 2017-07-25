package mil.nga.giat.geowave.core.store.callback;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public class IngestCallbackList<T, R extends GeoWaveRow> implements
		IngestCallback<T, R>,
		Flushable,
		Closeable
{
	private final List<IngestCallback<T,R>> callbacks;

	public IngestCallbackList(
			final List<IngestCallback<T,R>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryIngested(
			final T entry,
			R...kvs ) {
		for (final IngestCallback<T,R> callback : callbacks) {
			callback.entryIngested(
					entry,
					kvs);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final IngestCallback<T,R> callback : callbacks) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}

	@Override
	public void flush()
			throws IOException {
		for (final IngestCallback<T,R> callback : callbacks) {
			if (callback instanceof Flushable) {
				((Flushable) callback).flush();
			}
		}
	}

}
