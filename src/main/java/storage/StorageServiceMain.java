package storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;


public class StorageServiceMain {
	
	public static void main(String[] args) {
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
		cassandraHostConfigurator.setHosts("localhost:9160");
		
		StorageService cs = new StorageServiceImpl("Test Cluster", cassandraHostConfigurator, "user1");
		
		Map<String,String> buckets = new HashMap<String, String>();
		buckets.put("b1","bucketOne");
		buckets.put("b2","bucketTwo");
		buckets.put("b4","bucketThree");
		cs.addEntries("ns1","key1",buckets);
		
		List<String> l = new ArrayList<String>();
		l.add("b2");
		System.out.println(cs.getEntries("ns1", "key1"));
		System.out.println(cs.getEntries("ns1", "key1",l));	
	}

}
