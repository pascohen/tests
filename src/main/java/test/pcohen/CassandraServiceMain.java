package test.pcohen;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;


public class CassandraServiceMain {
	
	private static void fillDB(CassandraService cs) {
		Entry e1 = new Entry();
		e1.setUserId("user1");
		e1.setId("id1");
		e1.setRequest("req1-1");
		e1.setResponse("res1-1");
		e1.setTimestamp(10l);

		Entry e2 = new Entry();
		e2.setUserId("user1");
		e2.setId("id2");
		e2.setRequest("req1-2");
		e2.setResponse("res1-2");
		e2.setTimestamp(10l);	

		Entry e3 = new Entry();
		e3.setUserId("user2");
		e3.setId("id1");
		e3.setRequest("req2-1");
		e3.setResponse("res2-1");
		e3.setTimestamp(10l);
		
		cs.addEntry(e1,10);
		cs.addEntry(e2,-10);
		cs.addEntry(e3,10);

	}
	
	public static void main(String[] args) {
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
		cassandraHostConfigurator.setHosts("localhost:9160");
		
		CassandraService cs = new CassandraServiceWithCompositeKeyImpl("Test Cluster", cassandraHostConfigurator, "MyKeySpace");
		//CassandraService cs = new CassandraServiceWithSuperCfImpl("Test Cluster", cassandraHostConfigurator, "MyKeySpace");
		
		//fillDB(cs);
		System.out.println(cs.get("user1"));
		System.out.println(cs.get("user21","id2"));
		
			
	}

}
