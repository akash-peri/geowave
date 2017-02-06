package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldTypeStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class AccumuloSplitsProviderTest {
	private final static Logger LOGGER = Logger.getLogger(AccumuloSplitsProviderTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();
	final GeometryFactory factory = new GeometryFactory();
	AccumuloOperations accumuloOperations;
	AccumuloIndexStore indexStore;
	AccumuloAdapterStore adapterStore;
	AccumuloDataStatisticsStore statsStore;
	AccumuloDataStore mockDataStore;
	AccumuloSecondaryIndexDataStore secondaryIndexDataStore;
	SplitsProvider splitsProvider;

	/**
	 * public List<InputSplit> getSplits(
	 * 
	 * final DistributableQuery query, final QueryOptions queryOptions,
	 * 
	 * final IndexStore indexStore,
	 * 
	 * final AdapterIndexMappingStore adapterIndexMappingStore, final Integer
	 * minSplits, final Integer maxSplits )
	 */

	@Before
	public void setUp() {
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector("root", new PasswordToken(new byte[0]));
		} catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error("Failed to create mock accumulo connection", e);
		}
		accumuloOperations = new BasicAccumuloOperations(mockConnector);

		indexStore = new AccumuloIndexStore(accumuloOperations);

		adapterStore = new AccumuloAdapterStore(accumuloOperations);

		statsStore = new AccumuloDataStatisticsStore(accumuloOperations);

		secondaryIndexDataStore = new AccumuloSecondaryIndexDataStore(accumuloOperations, new AccumuloOptions());

		mockDataStore = new AccumuloDataStore(indexStore, adapterStore, statsStore, secondaryIndexDataStore,
				new AccumuloAdapterIndexMappingStore(accumuloOperations), accumuloOperations, accumuloOptions);
		splitsProvider = new AccumuloSplitsProvider();
	}

	@Test
	public void testPopulateIntermediateSplitsBasic() {
		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();
		final AccumuloAdapterIndexMappingStoreFactory adapterIndexMappingStoreFactory = new AccumuloAdapterIndexMappingStoreFactory();
		final AccumuloRequiredOptions adapterStoreFactoryOptions = (AccumuloRequiredOptions) adapterIndexMappingStoreFactory.createOptionsInstance();
		adapterStoreFactoryOptions.setInstance("instance");
		adapterStoreFactoryOptions.setZookeeper("zookeeper");
		adapterStoreFactoryOptions.setUser("user");
		adapterStoreFactoryOptions.setPassword("password");
		//this fails; look for a way to mock this out
		//specifically, mock out the store with mockito...
		final AdapterIndexMappingStore adapterIndexMappingStore = adapterIndexMappingStoreFactory.createStore(adapterStoreFactoryOptions);

		final Geometry testGeoFilter = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					31),
			new Coordinate(
					24,
					31),
			new Coordinate(
					24,
					33)
		});
		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);
				
		try {
			splitsProvider.getSplits(accumuloOperations, 
					query,
					new QueryOptions(
						adapter, 
						index, 
						-1, 
						null, 
						new String[] { "aaa", "bbb" }), 
					adapterStore, 
					statsStore,
					indexStore, 
					adapterIndexMappingStore, 
					1, 
					5);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

	protected static class TestGeometry {
		private final Geometry geom;
		private final String id;

		public TestGeometry(final Geometry geom, final String id) {
			this.geom = geom;
			this.id = id;
		}
	}

	protected static class TestGeometryAdapter extends AbstractDataAdapter<TestGeometry>
			implements StatisticsProvider<TestGeometry> {
		private static final ByteArrayId GEOM = new ByteArrayId("myGeo");
		private static final ByteArrayId ID = new ByteArrayId("myId");

		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public ByteArrayId[] getNativeFieldIds() {
				return new ByteArrayId[] { GEOM };
			}

			@Override
			public CommonIndexValue toIndexValue(final TestGeometry row) {
				return new GeometryWrapper(row.geom, new byte[0]);
			}

			@SuppressWarnings("unchecked")
			@Override
			public PersistentValue<Object>[] toNativeValues(final CommonIndexValue indexValue) {
				return new PersistentValue[] {
						new PersistentValue<Object>(GEOM, ((GeometryWrapper) indexValue).getGeometry()) };
			}

			@Override
			public byte[] toBinary() {
				return new byte[0];
			}

			@Override
			public void fromBinary(final byte[] bytes) {

			}
		};

		private final static EntryVisibilityHandler<TestGeometry> GEOMETRY_VISIBILITY_HANDLER = new FieldTypeStatisticVisibility<TestGeometry>(
				GeometryWrapper.class);
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<TestGeometry, Object>() {

			@Override
			public ByteArrayId getFieldId() {
				return ID;
			}

			@Override
			public Object getFieldValue(final TestGeometry row) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<TestGeometry, Object>>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>>();

		static {
			COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
			NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		}

		public TestGeometryAdapter() {
			super(COMMON_FIELD_HANDLER_LIST, NATIVE_FIELD_HANDLER_LIST);
		}

		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId("test");
		}

		@Override
		public boolean isSupported(final TestGeometry entry) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(final TestGeometry entry) {
			return new ByteArrayId(entry.id);
		}

		@SuppressWarnings("unchecked")
		@Override
		public FieldReader getReader(final ByteArrayId fieldId) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultReaderForClass(Geometry.class);
			} else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(final ByteArrayId fieldId) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultWriterForClass(Geometry.class);
			} else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		public DataStatistics<TestGeometry> createDataStatistics(final ByteArrayId statisticsId) {
			if (BoundingBoxDataStatistics.STATS_ID.equals(statisticsId)) {
				return new GeoBoundingBoxStatistics(getAdapterId());
			} else if (CountDataStatistics.STATS_ID.equals(statisticsId)) {
				return new CountDataStatistics<TestGeometry>(getAdapterId());
			}
			LOGGER.warn("Unrecognized statistics ID " + statisticsId.getString() + " using count statistic");
			return new CountDataStatistics<TestGeometry>(getAdapterId(), statisticsId);
		}

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(final ByteArrayId statisticsId) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public void setField(final PersistentValue<Object> fieldValue) {
					if (fieldValue.getId().equals(GEOM)) {
						geom = (Geometry) fieldValue.getValue();
					} else if (fieldValue.getId().equals(ID)) {
						id = (String) fieldValue.getValue();
					}
				}

				@Override
				public TestGeometry buildRow(final ByteArrayId dataId) {
					return new TestGeometry(geom, id);
				}
			};
		}

		@Override
		public ByteArrayId[] getSupportedStatisticsIds() {
			return SUPPORTED_STATS_IDS;
		}

		@Override
		public int getPositionOfOrderedField(final CommonIndexModel model, final ByteArrayId fieldId) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldId.equals(dimensionField.getFieldId())) {
					return i;
				}
				i++;
			}
			if (fieldId.equals(GEOM)) {
				return i;
			} else if (fieldId.equals(ID)) {
				return i + 1;
			}
			return -1;
		}

		@Override
		public ByteArrayId getFieldIdForPosition(final CommonIndexModel model, final int position) {
			if (position < model.getDimensions().length) {
				int i = 0;
				for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
					if (i == position) {
						return dimensionField.getFieldId();
					}
					i++;
				}
			} else {
				final int numDimensions = model.getDimensions().length;
				if (position == numDimensions) {
					return GEOM;
				} else if (position == (numDimensions + 1)) {
					return ID;
				}
			}
			return null;
		}
	}

	private final static ByteArrayId[] SUPPORTED_STATS_IDS = new ByteArrayId[] { BoundingBoxDataStatistics.STATS_ID,
			CountDataStatistics.STATS_ID };

	private static class GeoBoundingBoxStatistics extends BoundingBoxDataStatistics<TestGeometry> {

		@SuppressWarnings("unused")
		protected GeoBoundingBoxStatistics() {
			super();
		}

		public GeoBoundingBoxStatistics(final ByteArrayId dataAdapterId) {
			super(dataAdapterId);
		}

		@Override
		protected Envelope getEnvelope(final TestGeometry entry) {
			// incorporate the bounding box of the entry's envelope
			final Geometry geometry = entry.geom;
			if ((geometry != null) && !geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
			return null;
		}

	}
}
