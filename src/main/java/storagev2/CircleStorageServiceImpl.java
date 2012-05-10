package storagev2;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class CircleStorageServiceImpl extends StorageServiceImpl implements CircleStorageService {

	public CircleStorageServiceImpl(String clusterName,
			CassandraHostConfigurator cassandraHostConfigurator,
			String keySpaceName) {
		super(clusterName,cassandraHostConfigurator,keySpaceName);
	}

	@Override
	protected boolean contains(String circleId) {
		boolean contains = false;
		for (ColumnFamilyDefinition cfDef: keySpaceDef.getCfDefs()) {
			if (cfDef.getName().equals("Circle_"+circleId)) return true;
		}
		return contains;
	}
	
	@Override
	protected void checkColFamily(String circleId) {
		if (!contains(circleId)) {
			ColumnFamilyDefinition newCol = HFactory.createColumnFamilyDefinition(keySpace.getKeyspaceName(), "Circle_"+circleId);
			cluster.addColumnFamily(newCol);
		}
	}	
}
