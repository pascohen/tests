package test.pcohen;

public class Entry {
private String userId;
private String id;
private String request;
private String response;
private long timestamp;

public String getUserId() {
	return userId;
}
public void setUserId(String userId) {
	this.userId = userId;
}
public String getId() {
	return id;
}
public void setId(String id) {
	this.id = id;
}
public String getRequest() {
	return request;
}
public void setRequest(String request) {
	this.request = request;
}
public String getResponse() {
	return response;
}
public void setResponse(String response) {
	this.response = response;
}
public long getTimestamp() {
	return timestamp;
}
public void setTimestamp(long timestamp) {
	this.timestamp = timestamp;
}

@Override
public String toString() {
	return "Entry [userId=" + userId + ", id=" + id + ", request=" + request
			+ ", response=" + response + ", timestamp=" + timestamp
			+ "]";
}



}
