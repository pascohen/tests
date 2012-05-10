package storagev2;

import java.util.List;
import java.util.Map;

public interface CircleStorageService {
	
	void put(String cirecleId, String circleKey, Map<String,String> data);
	Map<String,String> get(String circleId,String circleKey);
	Map<String,Map<String,String>> get(String circleId);
	List<String> getKeys(String circleId);
}
