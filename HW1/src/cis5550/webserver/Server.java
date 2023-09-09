package cis5550.webserver;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class Server {
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Written by Ziyu Wang");
			return;
		}
		
        if (args.length != 2 && args.length != 0) {
            System.err.println("Usage: java SimpleWebServer <port>");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        String rootpath = args[1];

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("server is listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("socketget: " + rootpath);
                // Launch a new worker thread for each client connection
                Thread workerThread = new Thread(new Worker(clientSocket, rootpath));
                workerThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

	class Worker implements Runnable {
	    private Socket clientSocket;
	    private String rootpath;
	
	    public Worker(Socket clientSocket, String rootpath) {
	        this.clientSocket = clientSocket;
	        this.rootpath = rootpath;
	    }
//	    private static void sendErrorResponse(OutputStream outputStream, String statusCode, String message) throws IOException {
//            PrintWriter out = new PrintWriter(outputStream, true);
//            out.println("HTTP/1.1 " + statusCode);
//            out.println("Content-Type: text/plain");
//            out.println("Server: Server");
////            out.println("Content-Length: " + message.length());
////            out.println(message);
//            out.println();
////            out.println();
//            out.flush();
//        }
//        private static void sendHeaders(OutputStream outputStream, String statusCode, long contentLength, String contenttype) throws IOException {
//            PrintWriter out = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true);
//            out.println("HTTP/1.1 " + statusCode);
//            if (contenttype.equals("jpg") || contenttype.equals("jpeg")) {
//            	out.println("Content-Type: image/jpeg");
//            } else if (contenttype.equals("txt")) {
//            	out.println("Content-Type: text/plain");
//            } else if (contenttype.equals("html")) {
//            	out.println("Content-Type: text/html");
//            } else {
//            	out.println("Content-Type: application/octet-stream");
//            }
//            out.println("Server: Server");
////            out.println("Connection: keep-alive");
//            out.println("Content-Length: " + contentLength);
//            out.println();
////            out.println();
//            out.flush(); // Flush the headers to ensure they are sent before the file data
//        }
	    private static void sendErrorResponse(OutputStream outputStream, String statusCode, String message) throws IOException {
	        String response = "HTTP/1.1 " + statusCode + "\r\n" +
	                          "Content-Type: text/plain; charset=UTF-8\r\n" +
	                          "Server: Server\r\n" +
	                          "\r\n";

	        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
	        outputStream.write(responseBytes);
	        outputStream.flush();
	    }

	    private static void sendHeaders(OutputStream outputStream, String statusCode, long contentLength, String contentType) throws IOException {
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

        private static void sendFileData(OutputStream outputStream, File file) throws IOException {
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
	        try (
			InputStream inputStream = this.clientSocket.getInputStream();
	        OutputStream outputStream = this.clientSocket.getOutputStream();
	        ) {
    		byte[] buffer = new byte[1024];
            int bytesRead;
            boolean headersComplete = false;
            String url = "";
            ByteArrayOutputStream headerStream = new ByteArrayOutputStream();
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                // Process the incoming bytes
            	int i =0;
            	while (i<bytesRead){
                	headerStream.write(buffer[i]);
                    if (i > 2 && buffer[i] == 10 && buffer[i - 1] == 13 && buffer[i - 2] == 10 && buffer[i - 3] == 13) {
                        // Found the double CRLF, indicating the end of headers
                    	String headerString = new String(headerStream.toByteArray(), "UTF-8");
                        BufferedReader headerReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerString.getBytes("UTF-8"))));
                        
                        // Read and process the request line
                        String requestLine = headerReader.readLine();
                        System.out.println("Received: " + requestLine);
                        String[] requestParts = requestLine.split(" ");
                        System.out.println("lenght: " + requestParts.length);
                        if (requestParts.length != 3) {
                        	 sendErrorResponse(outputStream, "400 Bad Request", "header are missing from the request");
                             return;
                        }
                        
                    	String method = requestParts[0];
                        url = rootpath+requestParts[1];
                        String httpVersion = requestParts[2];
                       
                        if ( method.equals("POST") ||method.equals("PUT")) {
                        	sendErrorResponse(outputStream, "405 Not Allowed", "405 Not Allowed");
                            return;
                        }
                        if ( !method.equals("GET") && !method.equals("HEAD") && !method.equals("POST") && !method.equals("PUT")) {
                        	sendErrorResponse(outputStream, "501 Not Implemented", "501");
                            return;
                        }
                        if ( !httpVersion.equals("HTTP/1.1")) {
                        	sendErrorResponse(outputStream, "505 HTTP Version Not Supported", "505");
                            return;
                        }
                        if (url.contains("..")) {
                            // Respond with a 403 Forbidden
                            sendErrorResponse(outputStream, "403 Forbidden", "Forbidden");
                            return;
                        }
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
                        
                        // Process headers, look for Content-Length
                        String header;
                        int contentLength = 0;
                        while ((header = headerReader.readLine()) != null && !header.isEmpty()) {
                            // Process headers here, e.g., check for Content-Length
                        	System.out.println(url+"1Received: " + header);
                            if (header.startsWith("Content-Length: ")) {
                                contentLength = Integer.parseInt(header.substring("Content-Length: ".length()));
                                System.out.println("contentlenght: " + contentLength);
//	                                        alive=1;
                                // Handle content if present (for now, let's assume no message body)
                            }
                            if (header.contains("keep-alive")) {
//	                                        	alive=1;
                                // Handle content if present (for now, let's assume no message body)
                            }
                        }
                        if (contentLength > 0) {
                        	System.out.println("contentLength > 0");
                            // Read and discard the request body (for now)
                            System.out.println("contentLength > 2");
                            i=i+contentLength;
                            System.out.println("contentLength > 3");
                        }
                        // Headers are complete
                        headersComplete = true;
                        headerStream.reset();
                        //break;
                        
                    }
                    i=i+1;
                }
                if (headersComplete) {
                	File file = new File(url);
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
	}
//
//    private static void handleClientRequest(Socket clientSocket, String rootpath) throws IOException {
////    	int alive = 0;
//    	try (
//    			InputStream inputStream = clientSocket.getInputStream();
//    	        OutputStream outputStream = clientSocket.getOutputStream();
//    	    ) {
//    		byte[] buffer = new byte[1024];
//            int bytesRead;
//            boolean headersComplete = false;
//            while ((bytesRead = inputStream.read(buffer)) != -1) {
//                // Process the incoming bytes
//                for (int i = 0; i < bytesRead; i++) {
//                    if (i > 2 && buffer[i] == 10 && buffer[i - 1] == 13 && buffer[i - 2] == 10 && buffer[i - 3] == 13) {
//                        // Found the double CRLF, indicating the end of headers
//                        String headerString = new String(buffer, 0, i + 1, "UTF-8");
//                        BufferedReader headerReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerString.getBytes("UTF-8"))));
//                        
//                        // Read and process the request line
//                        String requestLine = headerReader.readLine();
//                        System.out.println("Received: " + requestLine);
//                        String[] requestParts = requestLine.split(" ");
//                        if (requestParts.length != 3) {
//                        	 sendErrorResponse(outputStream, "400 Bad Request", "header are missing from the request");
//                             return;
//                        }
//                        
//                    	String method = requestParts[0];
//                        String url = rootpath+requestParts[1];
//                        String httpVersion = requestParts[2];
//                       
//                        if ( method.equals("POST") ||method.equals("PUT")) {
//                        	sendErrorResponse(outputStream, "405 Not Allowed", "405 Not Allowed");
//                            return;
//                        }
//                        if ( !method.equals("GET") && !method.equals("HEAD") && !method.equals("POST") && !method.equals("PUT")) {
//                        	sendErrorResponse(outputStream, "501 Not Implemented", "501");
//                            return;
//                        }
//                        if ( !httpVersion.equals("HTTP/1.1")) {
//                        	sendErrorResponse(outputStream, "505 HTTP Version Not Supported", "505");
//                            return;
//                        }
//                        if (url.contains("..")) {
//                            // Respond with a 403 Forbidden
//                            sendErrorResponse(outputStream, "403 Forbidden", "Forbidden");
//                            return;
//                        }
//                        File file = new File(url);
//                        if (!file.exists()) {
//                            // 404 Not Found: File does not exist
//                            sendErrorResponse(outputStream, "404 Not Found", "404 Not Found");
//                            return;
//                        }
//
//                        if (!file.canRead()) {
//                            // 403 Forbidden: File is not readable
//                            sendErrorResponse(outputStream, "403 Forbidden", "403 Forbidden");
//                            return;
//                        }
//                        
//                        // Process headers, look for Content-Length
//                        String header;
//                        int contentLength = 0;
//                        while ((header = headerReader.readLine()) != null && !header.isEmpty()) {
//                            // Process headers here, e.g., check for Content-Length
//                        	System.out.println("Received: " + header);
//                            if (header.startsWith("Content-Length: ")) {
//                                contentLength = Integer.parseInt(header.substring("Content-Length: ".length()));
////                                alive=1;
//                                // Handle content if present (for now, let's assume no message body)
//                            }
//                            if (header.contains("keep-alive")) {
////                                	alive=1;
//                                // Handle content if present (for now, let's assume no message body)
//                            }
//                        }
//                        if (contentLength > 0) {
//                            // Read and discard the request body (for now)
//                            byte[] requestBody = new byte[contentLength];
//                            inputStream.read(requestBody);
//                        }
//                        // Headers are complete
//                        headersComplete = true;
//                        sendHeaders(outputStream, "200 OK", file.length());
//                        // Send file data
//                        sendFileData(outputStream, file);
//                        //break;
//                    }
//                }
//                if (headersComplete) {
//                    // Headers have been processed, exit the loop
//                    //break;
//                }
//            }
//    	}catch (IOException e) {
//            e.printStackTrace();
//        }
//            
//    }
//    private static void sendErrorResponse(OutputStream outputStream, String statusCode, String message) throws IOException {
//        PrintWriter out = new PrintWriter(outputStream, true);
//        out.println("HTTP/1.1 " + statusCode);
//        out.println("Content-Type: text/plain");
//        out.println("Server: MyServer");
//        out.println();
//        out.println(message);
//    }
//    private static void sendHeaders(OutputStream outputStream, String statusCode, long contentLength) throws IOException {
//        PrintWriter out = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true);
//        out.println("HTTP/1.1 " + statusCode);
//        out.println("Content-Type: text/plain"); // You can set the appropriate content type based on the file type
//        out.println("Server: MyServer");
//        out.println("Connection: keep-alive");
//        out.println("Content-Length: " + contentLength);
//        out.println();
//        out.flush(); // Flush the headers to ensure they are sent before the file data
//    }
//
//    private static void sendFileData(OutputStream outputStream, File file) throws IOException {
//        try (FileInputStream fileInputStream = new FileInputStream(file)) {
//            byte[] buffer = new byte[1024];
//            int bytesRead;
//            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
//                outputStream.write(buffer, 0, bytesRead);
//            }
//        }
//    }
//}
