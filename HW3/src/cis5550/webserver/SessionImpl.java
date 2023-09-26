package cis5550.webserver;

import java.util.Map;
import java.util.HashMap;

public class SessionImpl implements Session {
    private String sessionId;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;
    private Map<String, Object> attributes;

    public SessionImpl(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        this.maxActiveInterval = 300*1000;
        this.attributes = new HashMap<>();
    }
    public int getinterval() {
        return maxActiveInterval;
    }

    @Override
    public String id() {
        return sessionId;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }
    
    public void updateLastAccessTime(long time) {
        lastAccessedTime=time;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds*1000;
    }
    
    @Override
    public void invalidate() {
    	attributes.clear();
        lastAccessedTime = -1;
    }
    
    public boolean isvalid() {
    	if (this.lastAccessedTime+this.maxActiveInterval<=System.currentTimeMillis()) {
    		return false;
    	}
    		return true;
    	
    }
    @Override
    public Object attribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }
}
