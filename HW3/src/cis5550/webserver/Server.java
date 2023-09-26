package cis5550.webserver;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {
	private static Server serverInstance = null;
	private static boolean serverStarted = false;
	private static int port;
	private static int secureport;
	private static String path;
	private static Map<String, RouteEntry> routeTable = new HashMap<>();
	private static Map<String, Session> sessionTable = new HashMap<>();
	
	public void run() {
		Thread httpThread = new Thread(() -> startHttpServer());
        Thread httpsThread = new Thread(() -> startHttpsServer());
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> deletesessions(), 0, 5, TimeUnit.SECONDS);
        httpThread.start();
        httpsThread.start();
    }
//	public static void main(String[] args){
//		securePort(443);
//		get("/", (req,res) -> { return "Hello World - this is wangziyu"; });
//	}
	public static Map<String, Session> getsessionTable(){
		return sessionTable;
	}
	public static void deletesessions() {
	    Iterator<Map.Entry<String, Session>> iterator = sessionTable.entrySet().iterator();

	    while (iterator.hasNext()) {
	        Map.Entry<String, Session> entry = iterator.next();
	        Session session = entry.getValue();

	        if (!session.isvalid()) {
	            // Remove the invalidated session from the sessionTable
	            iterator.remove();
	        }
	    }
	}
	public static void startHttpsServer() {
		String pwd = "secret";
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, pwd.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
            SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
            SSLServerSocket serverSocketTLS = (SSLServerSocket) serverSocketFactory.createServerSocket(secureport);
            System.out.println("server is listening on port " + secureport);
            while (true) {
                Socket clientSocket = serverSocketTLS.accept();
                System.out.println("socketget: " + path);
                // Launch a new worker thread for each client connection
                Thread workerThread = new Thread(new Worker(clientSocket, path,routeTable,serverInstance));
                workerThread.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public static void startHttpServer() {
		try (ServerSocket serverSocket = new ServerSocket(port)) {
          System.out.println("server is listening on port " + port);
          while (true) {
              Socket clientSocket = serverSocket.accept();
              System.out.println("socketget: " + path);
              // Launch a new worker thread for each client connection
              Thread workerThread = new Thread(new Worker(clientSocket, path,routeTable,serverInstance));
              workerThread.start();
          }
      } catch (IOException e) {
          e.printStackTrace();
      }
	}
	
    private Server() {
        // Initialize the server with the given port
    }
	public static class staticFiles {
        public static void location(String s) {
        	path = s;
        }
    }
	public static void get(String path, Route route) {
		routeTable.put("GET" + path, new RouteEntry("GET",path,route));
		if(serverInstance == null){
			serverInstance = new Server();
		}
		if (!serverStarted) {
			Thread serverThread = new Thread(serverInstance::run);
			serverThread.start();
            serverStarted = true;
        }
    }

    public static void post(String path, Route route) {
    	routeTable.put("POST" + path, new RouteEntry("POST",path,route));
    	if(serverInstance == null){
			serverInstance = new Server();
		}
		if (!serverStarted) {
			Thread serverThread = new Thread(serverInstance::run);
			serverThread.start();
            serverStarted = true;
        }
    }

    public static void put(String path, Route route) {
    	routeTable.put("PUT" + path, new RouteEntry("PUT",path,route));
    	if(serverInstance == null){
			serverInstance = new Server();
		}
		if (!serverStarted) {
			Thread serverThread = new Thread(serverInstance::run);
			serverThread.start();
            serverStarted = true;
        }
    }

    public static void port(int ports) {
    	port = ports;
    }
    
    public static void securePort(int securePortNo) {  
    	secureport = securePortNo;
    }
}
 	class Worker implements Runnable {
	    private Socket clientSocket;
	    private String rootpath;
	    private Map<String, RouteEntry> routeTable;
	    private Server serverInstance;
	    private InetSocketAddress clientAddress;
	
	    public Worker(Socket clientSocket, String rootpath,Map<String, RouteEntry> routeTable,Server serverInstance) {
	        this.clientSocket = clientSocket;
	        this.rootpath = rootpath;
	        this.routeTable=routeTable;
	        this.serverInstance = serverInstance;
	        this.clientAddress=(InetSocketAddress) clientSocket.getRemoteSocketAddress();
	    	
	    }
	    private void sendErrorResponse(OutputStream outputStream, String statusCode, String message) throws IOException {
	        String response = "HTTP/1.1 " + statusCode + "\r\n" +
	                          "Content-Type: text/plain; charset=UTF-8\r\n" +
	                          "Server: Server\r\n" +
	                          "\r\n";

	        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
	        outputStream.write(responseBytes);
	        outputStream.flush();
	    }

	    private void sendHeaders(OutputStream outputStream, String statusCode, long contentLength, String contentType) throws IOException {
	        StringBuilder headers = new StringBuilder();
	        headers.append("HTTP/1.1 ").append(statusCode).append("\r\n");

	        if (contentType.equals("jpg") || contentType.equals("jpeg")) {
	            headers.append("Content-Type: image/jpeg\r\n");
	        } else if (contentType.equals("txt")) {
	            headers.append("Content-Type: text/plain\r\n");
	        } else if (contentType.equals("html")) {
	            headers.append("Content-Type: text/html\r\n");
	        } else {
	            headers.append("Content-Type: application/octet-stream\r\n");
	        }

	        headers.append("Server: Server\r\n");
	        headers.append("Content-Length: ").append(contentLength).append("\r\n");
	        headers.append("\r\n");

	        byte[] headersBytes = headers.toString().getBytes(StandardCharsets.UTF_8);
	        outputStream.write(headersBytes);
	        outputStream.flush();
	    }

        private void sendFileData(OutputStream outputStream, File file) throws IOException {
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
        }
	
	    @Override
	    public void run() {
	    	System.out.println("111111111");
	        try (
			InputStream inputStream = this.clientSocket.getInputStream();
	        OutputStream outputStream = this.clientSocket.getOutputStream();
	        ) {
    		byte[] buffer = new byte[1024];
            int bytesRead;
            boolean headersComplete = false;
            String url = "";
            String method  = "";
            String rt = "";
            String protocal = "";
            ByteArrayOutputStream headerStream = new ByteArrayOutputStream();
           
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                // Process the incoming bytes
            	System.out.println("2111111");
            	int i =0;
            	Map<String, String> requestHeaders = new HashMap<>();
            	String requestBody = "";
            	while (i<bytesRead){
                	headerStream.write(buffer[i]);
                	System.out.println("3111111");
                    if (i > 2 && buffer[i] == 10 && buffer[i - 1] == 13 && buffer[i - 2] == 10 && buffer[i - 3] == 13) {
                    	System.out.println("4111111");
                        // Found the double CRLF, indicating the end of headers
                    	String headerString = new String(headerStream.toByteArray(), "UTF-8");
                        BufferedReader headerReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerString.getBytes("UTF-8"))));
                        //BufferedReader headerReader = new BufferedReader(new StringReader(headerString));
                        // Read and process the request line
                        String requestLine = headerReader.readLine();
                        System.out.println("Received: " + requestLine);
                        String[] requestParts = requestLine.split(" ");
                        System.out.println("lenght: " + requestParts.length);
                        if (requestParts.length != 3) {
                        	 sendErrorResponse(outputStream, "400 Bad Request", "header are missing from the request");
                             return;
                        }
                        
                    	method = requestParts[0];
                        url = rootpath+requestParts[1];
                        rt = requestParts[1];
                        protocal = requestParts[2];
                        String httpVersion = requestParts[2];

                        if ( !httpVersion.equals("HTTP/1.1")) {
                        	sendErrorResponse(outputStream, "505 HTTP Version Not Supported", "505");
                            return;
                        }
                        if (url.contains("..")) {
                            // Respond with a 403 Forbidden
                            sendErrorResponse(outputStream, "403 Forbidden", "Forbidden");
                            return;
                        }
                        // Process headers, look for Content-Length
                        String header;
                        int contentLength = 0;
                        while ((header = headerReader.readLine()) != null && !header.isEmpty()) {
                        	System.out.println("5111111");
                            // Process headers here, e.g., check for Content-Length
                            if (header.startsWith("Content-Length: ")) {
                                contentLength = Integer.parseInt(header.substring("Content-Length: ".length()));
                                System.out.println("contentlenght: " + contentLength);
                            }
                            if (header.startsWith("Cookie: ")) {
                                String cookieString = header.substring("Cookie: ".length());
                                String[] cookies = cookieString.split("; ");
                                for (String cookie : cookies) {
                                    String[] cookieParts = cookie.split("=");
                                    if (cookieParts.length == 2) {
                                        String cookieName = cookieParts[0].trim();
                                        String cookieValue = cookieParts[1].trim();

                                        if (cookieName.equals("SessionID")) {
                                            if (Server.getsessionTable().containsKey(cookieValue) && Server.getsessionTable().get(cookieValue).isvalid()) {
                                                Session session = Server.getsessionTable().get(cookieValue);
                                                session.updateLastAccessTime(System.currentTimeMillis());
                                            }
                                        }
                                    }
                                }
                            }
                            String[] headerParts = header.split(": ");
                            if (headerParts.length == 2) {
                            	System.out.println("headerput:" + headerParts[0]+headerParts[1]);
                                requestHeaders.put(headerParts[0].toLowerCase(), headerParts[1]);
                            }
                        }
                        if (contentLength > 0) {
                            byte[] requestBodyBytes = Arrays.copyOfRange(buffer, i+1, i+contentLength+1);
                            i=i+contentLength+1;
                            requestBody = new String(requestBodyBytes, "UTF-8");
                            System.out.println("requestBody:"+requestBody);
                        }
                        // Headers are complete
                        headersComplete = true;
                        headerStream.reset();
                        //break;
                        
                    }
                    i=i+1;
                    
            	}
                if (headersComplete) {
                	System.out.println("777111");
                	Map<String, String> pathParams = new HashMap<>();
                	boolean matched = false;
                	Route handler = null;
                	String[] urlParts = rt.split("\\?");
                	String purepath1 = urlParts[0];
                	if (routeTable.containsKey(method+purepath1)) {
                		matched=true;
                		handler = routeTable.get(method + purepath1).getHandler();
                	}else {
                        for (Map.Entry<String, RouteEntry> entry : routeTable.entrySet()) {
                            RouteEntry routeEntry = entry.getValue();

                            String[] purepath = purepath1.split("/");
                            String[] routeParts = routeEntry.getPathPattern().split("/");
                            	
                            if (purepath.length == routeParts.length) {
                                matched= true;

                                for (int j = 0; j < purepath.length; j++) {
                                    if (routeParts[j].startsWith(":")) {
                                        // Named parameter found, store it in pathParams
                                        String paramName = routeParts[j].substring(1);
                                        pathParams.put(paramName, purepath[j]);
                                        System.out.println("pp:"+paramName+" "+purepath[j]);
                                    } else if (!purepath[j].equals(routeParts[j])) {
                                    	System.out.println(purepath[j]);
                                    	System.out.println(routeParts[j]);
                                    	matched = false;
                                        break;
                                    }
                                }
                                if (matched==true) {
                                	handler = routeEntry.getHandler();
                                	break;
                                }
                            }
                        }
                	}
                	System.out.println("match?:"+matched);
                	if (matched) {
                		System.out.println("contain!:");
                        String queryString = urlParts.length > 1 ? urlParts[1] : "";
                        Map<String, String> queryParameters = new HashMap<>();
                        String[] parameterPairs = queryString.split("&");
                        for (String pair : parameterPairs) {
                            String[] keyValue = pair.split("=");
                            if (keyValue.length == 2) {
                                String paramName = URLDecoder.decode(keyValue[0], "UTF-8");
                                String paramValue = URLDecoder.decode(keyValue[1], "UTF-8");
                                queryParameters.put(paramName, paramValue);
                            }
                        }
                        if (requestHeaders.containsKey("content-type") && requestHeaders.get("content-type").equals("application/x-www-form-urlencoded")) {
                        	parameterPairs = requestBody.split("&");
                            for (String pair : parameterPairs) {
                                String[] keyValue = pair.split("=");
                                if (keyValue.length == 2) {
                                    String paramName = URLDecoder.decode(keyValue[0], "UTF-8");
                                    String paramValue = URLDecoder.decode(keyValue[1], "UTF-8");
                                    queryParameters.put(paramName, paramValue);
                                }
                            }
                        }
                		RequestImpl reqob = new RequestImpl(method, rt, protocal, requestHeaders, queryParameters, pathParams, clientAddress, requestBody.getBytes(StandardCharsets.UTF_8), serverInstance,outputStream);
                		ResponseImpl resp = reqob.getResponse();
                		try {
                			String answer = (String) handler.handle(reqob, resp);
                			System.out.println("answer success");
                			StringBuilder headers = new StringBuilder();
                			if (resp.committed!= true) {
                				headers.append("HTTP/1.1 ").append("200").append(" OK").append("\r\n");
                    	        for (Map.Entry<String, String> entry : resp.headers.entrySet()) {
                    	            headers.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
                    	        }
                    	        if(answer!=null) {
                    				headers.append("Content-Length: ").append(answer.length()).append("\r\n");
                        	        headers.append("\r\n");
                        	        byte[] answerBytes = answer.toString().getBytes(StandardCharsets.UTF_8);
                        	        byte[] headersBytes = headers.toString().getBytes(StandardCharsets.UTF_8);
                        	        outputStream.write(headersBytes);
                        	        outputStream.write(answerBytes);
                    			}else {
                    				headers.append("\r\n\r\n");
                    				byte[] headersBytes = headers.toString().getBytes(StandardCharsets.UTF_8);
                        	        outputStream.write(headersBytes);
                        	        outputStream.flush();
                    			}
                    	        resp.committed= true;
                			}
                			else{
                				break;
                			}
                	        
                        } catch (Exception e) {
                            //
////                        	sendErrorResponse(outputStream, "404 Not Found", "404 Not Found");
//                            resp.status(500, "Internal Server Error");
//                            resp.body("An internal server error occurred.");
//                            resp.committed= true;
                            StringBuilder headers = new StringBuilder();
                            headers.append("HTTP/1.1 ").append("500").append(" Internal Server Error").append("\r\n").append("\r\n");;
                            byte[] headersBytes = headers.toString().getBytes(StandardCharsets.UTF_8);
                            outputStream.write(headersBytes);
                            outputStream.flush();
                            break;
                        }
                	}else {
	                	File file = new File(url);
                        if (!file.exists()) {
                            // 404 Not Found: File does not exist
                            sendErrorResponse(outputStream, "404 Not Found", "404 Not Found");
                            return;
                        }

                        if (!file.canRead()) {
                            // 403 Forbidden: File is not readable
                            sendErrorResponse(outputStream, "403 Forbidden", "403 Forbidden");
                            return;
                        }
	                    String fileName = url.substring(url.lastIndexOf('/') + 1);
	                    String fileExtension = "";
	                    int dotIndex = fileName.lastIndexOf('.');
	                    if (dotIndex >= 0 && dotIndex < fileName.length() - 1) {
	                        fileExtension = fileName.substring(dotIndex + 1);
	                    }
	                	sendHeaders(outputStream, "200 OK", file.length(),fileExtension);
	                    // Send file data
	                    sendFileData(outputStream, file);
	                    // Reset headerStream for the next request
	                    headerStream.reset();
	                    headersComplete = false;
                	}
                }
            }
           
	    } catch (IOException e) {
            e.printStackTrace();
        }
	        finally {
	            try {
	                // Close the clientSocket when the worker thread is done
	                clientSocket.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
		private void handler(RequestImpl reqob, ResponseImpl resp) {
			// TODO Auto-generated method stub
			
		}
	}