package storagev2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class StorageServiceImpl implements StorageService {

	protected Cluster cluster;
	protected KeyspaceDefinition keySpaceDef;
	protected Keyspace keySpace;

	private Keyspace createSchema(Cluster cluster, String keySpaceName) {
		keySpaceDef = cluster.describeKeyspace(keySpaceName);

		// If keyspace does not exist, the CFs don't exist either. => create
		// them.
		if (keySpaceDef == null) {
			System.out.println("Create new keyspace");

			keySpaceDef = HFactory.createKeyspaceDefinition(keySpaceName);
			cluster.addKeyspace(keySpaceDef);	
		}

		return HFactory.createKeyspace(keySpaceName, cluster);
	}

	public StorageServiceImpl(String clusterName,
			CassandraHostConfigurator cassandraHostConfigurator,
			String keySpaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName,
				cassandraHostConfigurator);
		keySpace = createSchema(cluster, keySpaceName);
	}

	protected boolean contains(String bucketName) {
		boolean contains = false;
		for (ColumnFamilyDefinition cfDef: keySpaceDef.getCfDefs()) {
			if (cfDef.getName().equals(bucketName)) return true;
		}
		return contains;
	}
	
	protected void checkColFamily(String bucketName) {
		if (!contains(bucketName)) {
			ColumnFamilyDefinition newCol = HFactory.createColumnFamilyDefinition(keySpace.getKeyspaceName(), bucketName);
			cluster.addColumnFamily(newCol);
		}
	}
	
	public void put(String bucketName, String key,
			Map<String, String> entries) {
		checkColFamily(bucketName);
		ColumnFamilyTemplate<String, String> cfTemplate = new ThriftColumnFamilyTemplate<String, String>(keySpace, bucketName, StringSerializer.get(),StringSerializer.get());
		ColumnFamilyUpdater<String, String> cfUpdater = cfTemplate.createUpdater(key);
		for (String colName: entries.keySet()) {
			//HColumn<String, String> column = HFactory.createColumn(bucketName, buckets.get(bucketName));
			//cfUpdater.setColumn(column);
			cfUpdater.setString(colName, entries.get(colName));
		}
		cfTemplate.update(cfUpdater);
		//cfUpdater.update();
		
	}

	public Map<String, String> get(String bucketName, String key) {
		if (!contains(bucketName)) return null;
		ColumnFamilyTemplate<String, String> cfTemplate = new ThriftColumnFamilyTemplate<String, String>(keySpace, bucketName, StringSerializer.get(),StringSerializer.get());
		ColumnFamilyResult<String,String> results = cfTemplate.queryColumns(key);
		Map<String, String>  result = new HashMap<String, String>();
		for (String colName: results.getColumnNames()) {
			result.put(colName, results.getString(colName));
		}
		return result;
	}

	public Map<String, String> get(String bucketName, String key,
			List<String> entriesKeys) {
		if (!contains(bucketName)) return null;
		ColumnFamilyTemplate<String, String> cfTemplate = new ThriftColumnFamilyTemplate<String, String>(keySpace, bucketName, StringSerializer.get(),StringSerializer.get());
		ColumnFamilyResult<String,String> results = cfTemplate.queryColumns(key,entriesKeys);
		Map<String, String>  result = new HashMap<String, String>();
		for (String colName: results.getColumnNames()) {
			result.put(colName, results.getString(colName));
		}
		return result;
	}

	public Map<String,Map<String, String>> get(String bucketName) {
		if (!contains(bucketName)) return null;
		RangeSlicesQuery<String, String, String> query = HFactory.createRangeSlicesQuery(keySpace, StringSerializer.get(), StringSerializer.get(),StringSerializer.get());
		query.setColumnFamily(bucketName);
		query.setRange(null, null,false,Integer.MAX_VALUE);
		
		QueryResult<OrderedRows<String,String,String>> rows = query.execute();
		OrderedRows<String,String,String> orderedRows = rows.get();
		Map<String,Map<String, String>> result = new HashMap<String, Map<String,String>>(orderedRows.getCount());
		for (Row<String,String,String> r: orderedRows) {
			List<HColumn<String, String>> cols = r.getColumnSlice().getColumns();
			Map<String,String> map = new HashMap<String, String>(cols.size());
			for(HColumn<String, String> col: cols) {
				map.put(col.getName(), col.getValue());
			}
			result.put(r.getKey(), map);
		}
		return result;
	}

	public List<String> getKeys(String bucketName) {
		if (!contains(bucketName)) return null;
		RangeSlicesQuery<String, String, String> query = HFactory.createRangeSlicesQuery(keySpace, StringSerializer.get(), StringSerializer.get(),StringSerializer.get());
		query.setColumnFamily(bucketName);
		query.setReturnKeysOnly();
		QueryResult<OrderedRows<String,String,String>> rows = query.execute();
		OrderedRows<String,String,String> orderedRows = rows.get();
		List<String> keys = new ArrayList<String>(orderedRows.getCount());
		for (Row<String,String,String> r: orderedRows) {
			keys.add(r.getKey());
		}
		return keys;
	}


}
