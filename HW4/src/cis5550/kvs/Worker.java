package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker {
    private static Map<String, Map<String, Row>> tableData = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -cp lib/webserver.jar cis5550.kvs.Coordinator <port>");
            return;
        }
        // Server webServer = new Server();

        int workerPort = Integer.parseInt(args[0]);
        String storageDirectory = args[1];
        String coordinatorAddress = args[2];

        // Check if the "id" file exists in the storage directory
        File idFile = new File(storageDirectory, "id");
        String workerId = "";

        if (idFile.exists()) {
            // Read the worker's ID from the "id" file
            try (BufferedReader reader = new BufferedReader(new FileReader(idFile))) {
                workerId = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        } else {
            // Generate a random worker ID (five random lowercase letters)
            workerId = generateRandomId(5);
            // Write the worker ID to the "id" file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(idFile))) {
                writer.write(workerId);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        int port = Integer.parseInt(args[0]);
        port(port);
        startPingThread(coordinatorAddress, workerId, workerPort);

        put("/data/:T/:R/:C", (req, res) -> {
            // Extract parameters from the request
            String tableName = req.params("T");
            String rowKey = req.params("R");
            String column = req.params("C");

            // Get the request body as bytes
            byte[] value = req.bodyAsBytes();

            // Check if the table exists in the data structure, create it if not
            tableData.putIfAbsent(tableName, new ConcurrentHashMap<>());

            // Get the table's data
            Map<String, Row> table = tableData.get(tableName);

            // Check if the row exists in the table, create it if not
            table.putIfAbsent(rowKey, new Row(rowKey));

            // Get the row
            Row row = table.get(rowKey);

            // Use a separate function to put the row with the value in the specified column
            putRow(row, column, value);

            // Return an "OK" response
            res.status(200, "200");
            return "OK";
        });

        get("/data/:T/:R/:C", (req, res) -> {
            // Extract parameters from the request
            String tableName = req.params("T");
            String rowKey = req.params("R");
            String column = req.params("C");

            // Get the request body as bytes
            // byte[] value = req.bodyAsBytes();

            // Check if the table exists in the data structure, create it if not
            if (!tableData.containsKey(tableName)) {
                res.status(404, "404");
                return "Table not found";
            }
            Map<String, Row> table = tableData.get(tableName);
            if (!table.containsKey(rowKey)) {
                res.status(404, "404");
                return "Row not found";
            }
            Row row = table.get(rowKey);

            // Use a separate function to put the row with the value in the specified column
            byte[] bodys = getRow(row, column);

            // Return an "OK" response
            res.status(200, "200");
            res.bodyAsBytes(bodys);
            return null;
        });
    }

    private static void putRow(Row row, String column, byte[] value) {
        // Put the value in the specified column
        row.put(column, value);
    }

    private static byte[] getRow(Row row, String column) {
        // Put the value in the specified column
        return row.getBytes(column);
    }

    private static String generateRandomId(int length) {
        final String alphabet = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(alphabet.length());
            sb.append(alphabet.charAt(index));
        }
        return sb.toString();
    }
}
