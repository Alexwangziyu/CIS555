package cis5550.webserver;

import java.util.*;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.*;
import java.security.SecureRandom;
import java.util.Random;
// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  Server server;
  ResponseImpl resp;
  
  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg,OutputStream outputStream) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
    resp = new ResponseImpl(outputStream);
  }
  public ResponseImpl getResponse() {
	  	return resp;
	  }
  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }

  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }
  public Map<String,String> params() {
    return params;
  }
  public Session session() {
	  if (headers.containsKey("cookie")) {
		  String[] cookies = headers.get("cookie").split("; ");
          for (String cookie : cookies) {
              String[] cookieParts = cookie.split("=");
              if (cookieParts.length == 2) {
                  String cookieName = cookieParts[0].trim();
                  String cookieValue = cookieParts[1].trim();
                  if (cookieName.equals("SessionID")) {
                      if (Server.getsessionTable().containsKey(cookieValue) && Server.getsessionTable().get(cookieValue).isvalid()) {
                    	  System.out.println(Server.getsessionTable().get(cookieValue).lastAccessedTime());
                    	  System.out.println(Server.getsessionTable().get(cookieValue).getinterval());
                    	  System.out.println(System.currentTimeMillis());
                          return Server.getsessionTable().get(cookieValue);
                      }                  	
                  }
              }
          }
	  }
	  String sid = generateSessionID();
	  Session ns = new SessionImpl(sid);
	  Server.getsessionTable().put(sid,ns);
	  resp.header("set-cookie", "SessionID="+sid+"; Expires="+String.valueOf(ns.lastAccessedTime()+ns.getinterval()));
	  return ns;
  }
  public String generateSessionID() {
	    String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_-+=";
	    Random random = new SecureRandom();
	    StringBuilder sessionId = new StringBuilder();
	    
	    for (int i = 0; i < 20; i++) {
	        int randomIndex = random.nextInt(CHARACTERS.length());
	        char randomChar = CHARACTERS.charAt(randomIndex);
	        sessionId.append(randomChar);
	    }
	    
	    return sessionId.toString();
	}
}
