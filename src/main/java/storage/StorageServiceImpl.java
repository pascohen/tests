package storage;

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
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class StorageServiceImpl implements StorageService {

	private Cluster cluster;
	private KeyspaceDefinition keySpaceDef;
	private Keyspace keySpace;

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

	private boolean contains(String namespace) {
		boolean contains = false;
		for (ColumnFamilyDefinition cfDef: keySpaceDef.getCfDefs()) {
			if (cfDef.getName().equals(namespace)) return true;
		}
		return contains;
	}
	
	private void checkNameSpace(String namespace) {
		if (!contains(namespace)) {
			ColumnFamilyDefinition newCol = HFactory.createColumnFamilyDefinition(keySpace.getKeyspaceName(), namespace);
			cluster.addColumnFamily(newCol);
		}
	}
	
	public void addEntries(String namespace, String key,
			Map<String, String> buckets) {
		checkNameSpace(namespace);
		ColumnFamilyTemplate<String, String> cfTemplate = new ThriftColumnFamilyTemplate<String, String>(keySpace, namespace, StringSerializer.get(),StringSerializer.get());
		ColumnFamilyUpdater<String, String> cfUpdater = cfTemplate.createUpdater(key);
		for (String bucketName: buckets.keySet()) {
			//HColumn<String, String> column = HFactory.createColumn(bucketName, buckets.get(bucketName));
			//cfUpdater.setColumn(column);
			cfUpdater.setString(bucketName, buckets.get(bucketName));
		}
		cfTemplate.update(cfUpdater);
		//cfUpdater.update();
		
	}

	public Map<String, String> getEntries(String namespace, String key) {
		checkNameSpace(namespace);
		ColumnFamilyTemplate<String, String> cfTemplate = new ThriftColumnFamilyTemplate<String, String>(keySpace, namespace, StringSerializer.get(),StringSerializer.get());
		ColumnFamilyResult<String,String> results = cfTemplate.queryColumns(key);
		Map<String, String>  result = new HashMap<String, String>();
		for (String colName: results.getColumnNames()) {
			result.put(colName, results.getString(colName));
		}
		return result;
	}

	public Map<String, String> getEntries(String namespace, String key,
			List<String> bucketNames) {
		checkNameSpace(namespace);
		ColumnFamilyTemplate<String, String> cfTemplate = new ThriftColumnFamilyTemplate<String, String>(keySpace, namespace, StringSerializer.get(),StringSerializer.get());
		ColumnFamilyResult<String,String> results = cfTemplate.queryColumns(key,bucketNames);
		Map<String, String>  result = new HashMap<String, String>();
		for (String colName: results.getColumnNames()) {
			result.put(colName, results.getString(colName));
		}
		return result;
	}
}
