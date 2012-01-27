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
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.SuperCfResult;
import me.prettyprint.cassandra.service.template.SuperCfUpdater;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HColumnFamily;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraServiceWithSuperCfImpl implements CassandraService {

	private static Keyspace createSchema(Cluster cluster, String keySpaceName) {
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keySpaceName);

		// If keyspace does not exist, the CFs don't exist either. => create
		// them.
		if (keyspaceDef == null) {
			System.out.println("Create new keyspace");

			ThriftCfDef cfDef = (ThriftCfDef)HFactory.createColumnFamilyDefinition(keySpaceName,REQUESTS_CF);
			cfDef.setColumnType( ColumnType.SUPER );
			cfDef.setDefaultValidationClass("AsciiType");
			cfDef.setComparatorType(ComparatorType.ASCIITYPE);
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

	public CassandraServiceWithSuperCfImpl(String clusterName,
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
		
		/*
		ThriftSuperCfTemplate<String, String, String> tpl = new ThriftSuperCfTemplate<String,String,String>(keySpace, REQUESTS_CF, StringSerializer.get(),StringSerializer.get(),StringSerializer.get());
		 SuperCfUpdater<String,String,String> sUpdater = tpl.createUpdater(userId,dcx);
		 sUpdater.setString(REQUEST_COL_NAME, request);
		 sUpdater.setString(RESPONSE_COL_NAME, response);
		 sUpdater.setString(TIMESTAMP_COL_NAME, Long.toString(ts));
		 tpl.update(sUpdater);
		 */
	
		HColumn<String, String> myRequestCol = HFactory.createColumn(
				REQUEST_COL_NAME, request, StringSerializer.get(),
				StringSerializer.get());
		if (lifetime > 0) myRequestCol.setTtl(lifetime);
		
		HColumn<String, String> myResponseCol = HFactory.createColumn(
				RESPONSE_COL_NAME, response, StringSerializer.get(),
				StringSerializer.get());
		if (lifetime > 0) myResponseCol.setTtl(lifetime);
		
		HColumn<String, String> myTimestampCol = HFactory.createColumn(
				TIMESTAMP_COL_NAME, Long.toString(ts), StringSerializer.get(),
				StringSerializer.get());
		if (lifetime > 0) myTimestampCol.setTtl(lifetime);
		
		
		List<HColumn<String, String>> columns = new ArrayList<HColumn<String,String>>();
		columns.add(myRequestCol);
		columns.add(myResponseCol);
		columns.add(myTimestampCol);
		
		HSuperColumn<String, String, String> myRequestSuperCol = HFactory.createSuperColumn(id, columns, StringSerializer.get(),StringSerializer.get(),StringSerializer.get());
	 
		 Mutator<String> m = HFactory.createMutator(keySpace, StringSerializer.get());
		 m.insert(userId, REQUESTS_CF, myRequestSuperCol);
	}

	public Entry get(String userId, String id) {
		ThriftSuperCfTemplate<String, String, String> tpl = new ThriftSuperCfTemplate<String,String,String>(keySpace, REQUESTS_CF, StringSerializer.get(),StringSerializer.get(),StringSerializer.get());
		SuperCfResult<String, String, String> cfResult = tpl.querySuperColumn(userId, id);
		if ((cfResult != null) && (cfResult.hasResults())){
			Entry e = new Entry();
			e.setUserId(userId);
			e.setId(id);
			String request = StringSerializer.get().fromByteBuffer(
					(ByteBuffer) cfResult.getColumn(REQUEST_COL_NAME).getValue());
			e.setRequest(request);
			
			String response = StringSerializer.get().fromByteBuffer(
					(ByteBuffer) cfResult.getColumn(RESPONSE_COL_NAME).getValue());			
			e.setResponse(response);
			
			String timestamp = StringSerializer.get().fromByteBuffer(
					(ByteBuffer) cfResult.getColumn(TIMESTAMP_COL_NAME).getValue());

			e.setTimestamp(Long.parseLong(timestamp));
			return e;
		}
		return null;
	}
	

	public Map<String, Entry> get(String userId) {
		ThriftSuperCfTemplate<String, String, String> tpl = new ThriftSuperCfTemplate<String,String,String>(keySpace, REQUESTS_CF, StringSerializer.get(),StringSerializer.get(),StringSerializer.get());
		SuperCfResult<String, String, String> cfResult = tpl.querySuperColumns(userId);
		if ((cfResult != null) && (cfResult.hasResults())){
			Map<String, Entry> results = new HashMap<String, Entry>();
			Collection<String> keys = cfResult.getSuperColumns();
			for (String k:keys) {
				Entry e = new Entry();
				e.setUserId(userId);
				e.setId(k);
				String req = StringSerializer.get().fromByteBuffer(cfResult.getByteBuffer(k, REQUEST_COL_NAME));
				e.setRequest(req);
				String resp = StringSerializer.get().fromByteBuffer(cfResult.getByteBuffer(k, RESPONSE_COL_NAME));
				e.setResponse(resp);
				String ts = StringSerializer.get().fromByteBuffer(cfResult.getByteBuffer(k, TIMESTAMP_COL_NAME));
				e.setTimestamp(Long.parseLong(ts));
				results.put(k, e);
			}
			return results;
		}
		return null;
	}

}
