package storagev2;

import java.util.List;
import java.util.Map;

public interface StorageService {
	void put(String bucketName, String key, Map<String,String> entries);
	Map<String,String> get(String bucketName,String key);
	Map<String,String> get(String bucketName,String key, List<String> entriesKeys);
	Map<String,Map<String,String>> get(String bucketName);
	List<String> getKeys(String bucketName);
	

}
