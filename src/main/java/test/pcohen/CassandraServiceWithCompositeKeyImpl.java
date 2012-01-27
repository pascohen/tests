package test.pcohen;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class CassandraServiceWithCompositeKeyImpl implements CassandraService {

	private static Keyspace createSchema(Cluster cluster, String keySpaceName) {
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keySpaceName);

		// If keyspace does not exist, the CFs don't exist either. => create
		// them.
		if (keyspaceDef == null) {
			System.out.println("Create new keyspace");

			ColumnFamilyDefinition cfDef = HFactory
					.createColumnFamilyDefinition(keySpaceName, REQUESTS_CF,
							ComparatorType.ASCIITYPE);
			cfDef.setDefaultValidationClass("AsciiType");
			cfDef.setKeyValidationClass("AsciiType");
			
			List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
			cfDefs.add(cfDef);
			keyspaceDef = HFactory.createKeyspaceDefinition(keySpaceName,
					ThriftKsDef.DEF_STRATEGY_CLASS, 1, cfDefs);
			cluster.addKeyspace(keyspaceDef, true);
		}

		return HFactory.createKeyspace(keySpaceName, cluster);
	}

	private Cluster cluster;
	private Keyspace keySpace;

	public CassandraServiceWithCompositeKeyImpl(String clusterName,
			CassandraHostConfigurator cassandraHostConfigurator,
			String keySpaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName,
				cassandraHostConfigurator);
		keySpace = createSchema(cluster, keySpaceName);
	}

	public static final String REQUESTS_CF = "Requests";
	public static final String REQUEST_COL_NAME = "request";
	public static final String RESPONSE_COL_NAME = "response";
	public static final String TIMESTAMP_COL_NAME = "timestamp";
	public static final String USER_COL_NAME = "user";

	public void addEntry(Entry entry, int lifetime) {
		String userId = entry.getUserId();
		String id = entry.getId();
		String request = entry.getRequest();
		String response = entry.getResponse();
		long ts = entry.getTimestamp();

		// No Single String but a composite with a single value to simplify the
		// template requests
		/*
		 * Composite userCol = new Composite(); userCol.add(USER_COL_NAME);
		 * HColumn<Composite, String> myUserCol = HFactory.createColumn(userCol,
		 * userId, new CompositeSerializer(), StringSerializer.get());
		 * Mutator<String> muser = HFactory.createMutator(keySpace,
		 * StringSerializer.get()); muser.insert(userId,
		 * REQUESTS_CF,CassandraHostConfigurator myUserCol);
		 */

		Composite requestCol = new Composite();
		requestCol.add(id);
		requestCol.add(REQUEST_COL_NAME);

		HColumn<Composite, String> myRequestCol = HFactory.createColumn(
				requestCol, request, new CompositeSerializer(),
				StringSerializer.get());
		if (lifetime > 0)
			myRequestCol.setTtl(lifetime);
		Mutator<String> mrequest = HFactory.createMutator(keySpace,
				StringSerializer.get());
		mrequest.insert(userId, REQUESTS_CF, myRequestCol);

		Composite responseCol = new Composite();
		responseCol.add(id);
		responseCol.add(RESPONSE_COL_NAME);

		HColumn<Composite, String> myResponseCol = HFactory.createColumn(
				responseCol, response, new CompositeSerializer(),
				StringSerializer.get());
		if (lifetime > 0)
			myResponseCol.setTtl(lifetime);
		Mutator<String> mresponse = HFactory.createMutator(keySpace,
				StringSerializer.get());
		mresponse.insert(userId, REQUESTS_CF, myResponseCol);

		Composite tsCol = new Composite();
		tsCol.add(id);
		tsCol.add(TIMESTAMP_COL_NAME);

		// No LongSerializer to simplify the template requests
		HColumn<Composite, String> myTsCol = HFactory.createColumn(tsCol,
				Long.toString(ts), new CompositeSerializer(),
				StringSerializer.get());
		if (lifetime > 0)
			myTsCol.setTtl(lifetime);
		Mutator<String> mts = HFactory.createMutator(keySpace,
				StringSerializer.get());
		mts.insert(userId, REQUESTS_CF, myTsCol);
	}

	public Entry get(String userId, String id) {
		Entry e = new Entry();
		e.setId(id);
		e.setUserId(userId);

		ColumnQuery<String, Composite, String> colQuery = HFactory
				.createColumnQuery(keySpace, StringSerializer.get(),
						new CompositeSerializer(), StringSerializer.get());
		colQuery.setColumnFamily(REQUESTS_CF);
		colQuery.setKey(userId);

		Composite requestCol = new Composite();
		requestCol.add(id);
		requestCol.add(REQUEST_COL_NAME);

		colQuery.setName(requestCol);
		QueryResult<HColumn<Composite, String>> result = colQuery.execute();
		if (result.get() != null) {
			e.setRequest(result.get().getValue());
		} else {
			return null;
		}

		Composite responseCol = new Composite();
		responseCol.add(id);
		responseCol.add(RESPONSE_COL_NAME);

		colQuery.setName(requestCol);
		QueryResult<HColumn<Composite, String>> response = colQuery.execute();
		if (response.get() != null) {
			e.setResponse(response.get().getValue());
		} else {
			return null;
		}

		Composite tsCol = new Composite();
		tsCol.add(id);
		tsCol.add(TIMESTAMP_COL_NAME);
		colQuery.setName(tsCol);
		QueryResult<HColumn<Composite, String>> ts = colQuery.execute();
		if (ts.get() != null) {
			e.setTimestamp(Long.parseLong(ts.get().getValue()));
		} else {
			return null;
		}
		return e;
	}

	public Map<String, Entry> get(String userId) {

		Map<String, Entry> result = new HashMap<String, Entry>();

		ColumnFamilyTemplate<String, Composite> template = new ThriftColumnFamilyTemplate<String, Composite>(
				keySpace, REQUESTS_CF, StringSerializer.get(),
				new CompositeSerializer());
		ColumnFamilyResult<String, Composite> res = template
				.queryColumns(userId);
		Collection<Composite> cols = res.getColumnNames();
		for (Composite c : cols) {
			String key = StringSerializer.get().fromByteBuffer(
					(ByteBuffer) c.get(0));
			if (USER_COL_NAME.equals(key))
				continue;
			String type = StringSerializer.get().fromByteBuffer(
					(ByteBuffer) c.get(1));

			Entry e;
			e = result.get(key);
			if (e == null) {
				e = new Entry();
				e.setId(key);
				e.setUserId(userId);
				result.put(key, e);
			}
			if (REQUEST_COL_NAME.equals(type)) {
				e.setRequest(res.getString(c));
			}
			if (RESPONSE_COL_NAME.equals(type)) {
				e.setResponse(res.getString(c));
			}
			if (TIMESTAMP_COL_NAME.equals(type)) {
				e.setTimestamp(Long.parseLong(res.getString(c)));
			}
		}

		return result;
	}

}
