package test.pcohen;

import java.util.Map;

public interface CassandraService {
void addEntry(Entry entry, int lifetime);
Entry get(String userId, String id);
Map<String,Entry> get(String userId);
}
