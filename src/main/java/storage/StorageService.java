package storage;

import java.util.List;
import java.util.Map;

public interface StorageService {
	
	void addEntries(String namespace, String key, Map<String,String> buckets);
	Map<String,String> getEntries(String namespace,String key);
	Map<String,String> getEntries(String namespace,String key, List<String> bucketNames);

}
