package storagev2;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;


public class StorageServiceMain {
	
	public static void main(String[] args) {
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
		cassandraHostConfigurator.setHosts("localhost:9160");
		
		StorageService cs = new StorageServiceImpl("Test Cluster", cassandraHostConfigurator, "ksp1");
		CircleStorageService circles = new CircleStorageServiceImpl("Test Cluster", cassandraHostConfigurator, "ksp1");

		
		Map<String,String> buckets = new HashMap<String, String>();
		buckets.put("e1","entryOne");
		buckets.put("e2","entryTwo");
		buckets.put("e3","entryThree");
		cs.put("bucket1","key1",buckets);

		Map<String,String> buckets2 = new HashMap<String, String>();
		buckets2.put("e21","entry2One");
		buckets2.put("e22","entry2Two");
		cs.put("bucket1","key2",buckets2);

		
		System.out.println(cs.get("bucket1"));
		System.out.println(cs.getKeys("bucket1"));
		
		circles.put("c1","key1",buckets);
		circles.put("c1","key2",buckets2);

		System.out.println(circles.get("bucket1"));
		System.out.println(circles.getKeys("bucket1"));

	}

}
