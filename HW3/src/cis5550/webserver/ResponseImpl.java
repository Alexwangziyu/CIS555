package cis5550.webserver;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class ResponseImpl implements Response {
    private OutputStream outputStream;
    private int statusCode = 200;
    private String reasonPhrase = "OK";
    public Map<String, String> headers = new HashMap<>();
    private byte[] responseBody;
    public boolean committed = false;
 
    public ResponseImpl(OutputStream outputStream) {
        this.outputStream = outputStream;
        
    }

    @Override
    public void body(String body) {
        this.responseBody = body.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        this.responseBody = bodyArg;
    }

    @Override
    public void header(String name, String value) {
    	System.out.println("resput:"+name.toLowerCase()+value);
        headers.put(name.toLowerCase(), value);
    }

    @Override
    public void type(String contentType) {
        headers.put("Content-Type", contentType);
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    @Override
    public void write(byte[] b) throws Exception {

        if (!committed) {
            // If this is the first write, 'commit' the response
            StringBuilder header = new StringBuilder();
            header.append("HTTP/1.1 ").append(statusCode).append(" ").append(reasonPhrase).append("\r\n");
            for (String key : headers.keySet()) {
                String value = headers.get(key);
                header.append(key).append(": ").append(value).append("\r\n");
            }
            // Add 'Connection: close' header
            header.append("Connection: close\r\n\r\n");
            // Send headers to the connection
            outputStream.write(header.toString().getBytes(StandardCharsets.UTF_8));
            
            // Mark the response as committed
            committed = true;
        }
        else {
        	System.out.println("comt"+b.toString());
        }
//        // Write the provided bytes directly to the connection
        outputStream.write(b);
        outputStream.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        // Implement redirection (extra credit)
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        // Implement halting (extra credit)
    }
}
