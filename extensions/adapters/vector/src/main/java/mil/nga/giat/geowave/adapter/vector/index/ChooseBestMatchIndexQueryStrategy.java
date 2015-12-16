package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

public class ChooseBestMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{
	public static final String NAME = "Best Match";

	public String toString() {
		return NAME;
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final CloseableIterator<Index<?, ?>> indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			PrimaryIndex nextIdx = null;
			boolean done = false;

			@Override
			public boolean hasNext() {
				long min = Long.MAX_VALUE;
				PrimaryIndex bestIdx = null;

				while (!done && indices.hasNext()) {
					final Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						List<ByteArrayRange> ranges = DataStoreUtils.constraintsToByteArrayRanges(
								query.getIndexConstraints(nextIdx.getIndexStrategy()),
								nextIdx.getIndexStrategy(),
								5000);
						final long temp = DataStoreUtils.cardinality(
								nextIdx,
								stats,
								ranges);
						if (temp < min) {
							bestIdx = nextIdx;
							min = temp;
						}
					}
				}
				nextIdx = bestIdx;
				done = true;
				return nextIdx != null;
			}

			@Override
			public Index<?, ?> next()
					throws NoSuchElementException {
				if (nextIdx == null) throw new NoSuchElementException();
				Index<?, ?> returnVal = nextIdx;
				nextIdx = null;
				return returnVal;
			}

			@Override
			public void remove() {}

			@Override
			public void close()
					throws IOException {
				indices.close();
			}
		};
	}
}
