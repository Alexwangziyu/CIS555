package cis5550.generic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Worker {
    public static void startPingThread(String coordinatorURL, String workerId, int workerPort) {
        long interval = 5000; // 30 seconds

        // Create and start a new thread
        Thread pingThread = new Thread(() -> {
            while (true) {
                try {
                    // Construct the URL for pinging the coordinator
                    String pingURL = "http://" + coordinatorURL + "/ping?id=" + workerId + "&port=" + workerPort;
                    // Create a HttpURLConnection and set request method to GET
                    URL url = new URL(pingURL);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");
                    // Get the HTTP response code
                    int responseCode = connection.getResponseCode();

                    // Read the response content
                    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    reader.close();
                    // Print response code and content for testing (you can remove this in
                    // production)
                    System.out.println("Response Code: " + responseCode);
                    System.out.println("Response Content: " + response.toString());
                    // Sleep for the specified interval
                    Thread.sleep(interval);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Start the ping thread
        pingThread.start();
    }
}
