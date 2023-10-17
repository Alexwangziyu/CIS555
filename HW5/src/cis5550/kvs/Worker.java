package cis5550.kvs;
import static cis5550.webserver.Server.*;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Worker extends cis5550.generic.Worker {
    private static Map<String, Map<String, Row>> tableData = new ConcurrentHashMap<>();
    private static String storageDirectory;
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -cp lib/webserver.jar cis5550.kvs.Coordinator <port>");
            return;
        }
        // Server webServer = new Server();

        int workerPort = Integer.parseInt(args[0]);
        storageDirectory = args[1];
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
            if (tableName.startsWith("pt-")) {
            	//when tableName.startsWith("pt-"), we dont store the table in memory
            	//we should update or put the new value in file
            	updateOnDisk(tableName,rowKey,column,value);
            } else {
                // Check if the table exists in the data structure, create it if not
                tableData.putIfAbsent(tableName, new ConcurrentHashMap<>());

                // Get the table's data
                Map<String, Row> table = tableData.get(tableName);

                // Check if the row exists in the table, create it if not
                table.putIfAbsent(rowKey, new Row(rowKey));

                // Get the row
                Row row = table.get(rowKey);
                putRow(row, column, value);
            }
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
            if (tableName.startsWith("pt-")) {
            	String directoryPath = storageDirectory + "/" + tableName;
                Row newrow;
                File directory = new File(directoryPath);
                if (!directory.exists()) {
                	res.status(404, "404");
                    return "Table not found1";
                }
                
                String fileName = rowKey;
                File file = new File(directoryPath, fileName);
                if (file.exists()) {
                	FileInputStream fi = new FileInputStream(file);
                	newrow=Row.readFrom(fi);
                	byte[] bodys = getRow(newrow,column);
                	res.status(200, "200");
                    res.bodyAsBytes(bodys);
                    return null;
                }
                else {
                	res.status(404, "404");
                    return "Table not found2";
                }
            }
            else {
            	if (!tableData.containsKey(tableName)) {
                    res.status(404, "404");
                    return "Table not found3";
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
            }
        });
        
        
        get("/data/:T/:R", (req, res) -> {
            // Extract parameters from the request
            String tableName = req.params("T");
            String rowKey = req.params("R");

            // Get the request body as bytes
            // byte[] value = req.bodyAsBytes();

            // Check if the table exists in the data structure, create it if not
            if (tableName.startsWith("pt-")) {
            	String directoryPath = storageDirectory + "/" + tableName;
                Row newrow;
                File directory = new File(directoryPath);
                if (!directory.exists()) {
                	res.status(404, "404");
                    return "Table not found1";
                }
                
                String fileName = rowKey;
                File file = new File(directoryPath, fileName);
                if (file.exists()) {
                	FileInputStream fi = new FileInputStream(file);
                	newrow=Row.readFrom(fi);
                	byte[] bodys = newrow.toByteArray();
                	res.status(200, "200");
                    res.bodyAsBytes(bodys);
                    return null;
                }
                else {
                	res.status(404, "404");
                    return "Table not found2";
                }
            }
            else {
            	if (!tableData.containsKey(tableName)) {
                    res.status(404, "404");
                    return "Table not found3";
                }
                Map<String, Row> table = tableData.get(tableName);
                if (!table.containsKey(rowKey)) {
                    res.status(404, "404");
                    return "Row not found";
                }
                Row row = table.get(rowKey);

                // Use a separate function to put the row with the value in the specified column
                byte[] bodys = row.toByteArray();

                // Return an "OK" response
                res.status(200, "200");
                res.bodyAsBytes(bodys);
                return null;
            }
        });
        
        
        get("/data/:T", (req, res) -> {
            // Extract parameters from the request
            String tableName = req.params("T");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            // Get the request body as bytes
            // byte[] value = req.bodyAsBytes();

            // Check if the table exists in the data structure, create it if not

            if (tableName.startsWith("pt-")) {
                // Check if the table exists in the data structure, create it if not
                String directoryPath = storageDirectory + "/" + tableName;
                File directory = new File(directoryPath);

                if (!directory.exists()) {
                    res.status(404, "404");
                    return "Table not found";
                }
                byte[] newline = "\n".getBytes(StandardCharsets.UTF_8);
                // Stream the content of all rows in the table
                int count = 0;
                for (File file : directory.listFiles()) {
                    if (file.isFile()) {
                    	FileInputStream fi = new FileInputStream(file);
                        Row newRow = Row.readFrom(fi);
                    	String rowKey = newRow.key();
                    	if ((startRow == null || rowKey.compareTo(startRow) >= 0) &&
                                (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
                    		count=1;
                            byte[] bodyBytes = newRow.toByteArray();
                            // Send each row followed by a LF
                            res.write(bodyBytes);
                            res.write(newline);
                    		
                    	}
                    }
                }
                if (count ==0) {
                	res.status(404, "404");
                    return "No satisfied";
                }
                // Send an additional LF character to indicate the end of the stream
                res.write(newline);
                res.status(200, "200");
                return null;
            }
            else {
            	 // Check if the table exists in the data structure
                if (!tableData.containsKey(tableName)) {
                    res.status(404, "404");
                    return "Table not found";
                }

                Map<String, Row> table = tableData.get(tableName);
                byte[] newline = "\n".getBytes(StandardCharsets.UTF_8);
                // Set the response content type to text/plain
                res.type("text/plain");
                int count = 0;
                try {
                    // Stream the content of all rows in the table
                    for (Row row : table.values()) {
                        byte[] bodyBytes = row.toByteArray();
                        String rowKey = row.key();
                        if ((startRow == null || rowKey.compareTo(startRow) >= 0) &&
                                (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
                        // Send each row followed by a LF using response's write() method
                        count=1;
                        res.write(bodyBytes);
                        res.write(newline);
                        }
                    }
                    if (count ==0) {
                    	res.status(404, "404");
                        return "No satisfied";
                    }

                    // Send an additional LF character to indicate the end of the stream
                    res.write(newline);
                } catch (IOException e) {
                    e.printStackTrace();
                    res.status(500, "500");
                    return "Internal Server Error";
                }

                res.status(200, "200");
                return null;
            
            }
        });
        
        get("/count/:T", (req, res) -> {
            String tableName = req.params("T");

            if (tableName.startsWith("pt-")) {
                // For persistent tables, count the files in the corresponding directory
                String directoryPath = storageDirectory + "/" + tableName;
                File directory = new File(directoryPath);

                if (!directory.exists()) {
                    res.status(404,"404");
                    return "Table not found";
                }

                // Count the files in the directory
                int fileCount = directory.listFiles().length;

                // Return the file count as a response
                res.status(200,"200");
                return String.valueOf(fileCount);
            } else {
                // For in-memory tables, count the rows in the data structure
                if (!tableData.containsKey(tableName)) {
                    res.status(404,"404");
                    return "Table not found";
                }

                Map<String, Row> table = tableData.get(tableName);

                // Count the rows in the table
                int rowCount = table.size();

                // Return the row count as a response
                res.status(200,"200");
                return String.valueOf(rowCount);
            }
        });
        
        put("/rename/:T", (req, res) -> {
            String tableName = req.params("T");
            String newTableName = req.body();
            
            if (newTableName == null || newTableName.isEmpty()) {
                res.status(400,"400");
                return "Invalid new name";
            }
         
            if (tableName.startsWith("pt-")) {
                // For persistent tables, rename the directory
                String oldDirectoryPath = storageDirectory + "/" + tableName;
                String newDirectoryPath = storageDirectory + "/" + newTableName;

                File oldDirectory = new File(oldDirectoryPath);
                File newDirectory = new File(newDirectoryPath);

                if (!oldDirectory.exists()) {
                    res.status(404,"404");
                    return "Table not found";
                }
                if (newDirectory.exists()) {
                    res.status(409,"409");
                    return "New name already exists";
                }
                if (!newTableName.startsWith("pt-")) {
                	res.status(400,"400");
                    return "st- error";
                }
                // Rename the directory
                if (oldDirectory.renameTo(newDirectory)) {
                    res.status(200,"200");
                    return "OK";
                } else {
                    res.status(500,"500");
                    return "Failed to rename table";
                }
            } else {
                // For in-memory tables, rename the data structure
                if (!tableData.containsKey(tableName)) {
                    res.status(404,"404");
                    return "Table not found";
                }
                if (newTableName.startsWith("pt-")) {
                	res.status(400,"400");
                    return "st- error";
                }
                Map<String, Row> table = tableData.get(tableName);
                // Create a new table with the new name and copy the data
                tableData.put(newTableName, new ConcurrentHashMap<>(table));
                tableData.remove(tableName);
                res.status(200,"200");
                return "OK";
            }
        });

        put("/delete/:T", (req, res) -> {
            String tableName = req.params("T");

            if (tableName.startsWith("pt-")) {
                // For persistent tables, delete the directory and its contents
                String directoryPath = storageDirectory + "/" + tableName;
                File directory = new File(directoryPath);

                if (!directory.exists()) {
                    res.status(404,"404");
                    return "Table not found";
                }
                // Delete all files in the directory
                for (File file : directory.listFiles()) {
                    if (file.isFile()) {
                        file.delete();
                    }
                }
                // Delete the directory itself
                if (directory.delete()) {
                    res.status(200,"200");
                    return "Table deleted successfully";
                } else {
                    res.status(500,"500");
                    return "Failed to delete table";
                }
            } else {
                // For in-memory tables, remove the table from the data structure
                if (!tableData.containsKey(tableName)) {
                    res.status(404,"404");
                    return "Table not found";
                }
                tableData.remove(tableName);
                res.status(200,"200");
                return "Table deleted successfully";
            }
        });
        
        get("/", (req, res) -> {
            StringBuilder responseContent = new StringBuilder("<html><body>");
            responseContent.append("<h1>List of Data Tables</h1>");
            responseContent.append("<table border='1'><tr><th>Table Name</th><th>Number of Keys</th></tr>");

            // Iterate through the data tables and generate rows for each table
            for (Map.Entry<String, Map<String, Row>> entry : tableData.entrySet()) {
                String tableName = entry.getKey();
                Map<String, Row> tableRows = entry.getValue();
                int numKeys = tableRows.size();

                // Create a row in the HTML table with the table name and a hyperlink
                responseContent.append("<tr>");
                responseContent.append("<td><a href='/view/").append(tableName).append("'>").append(tableName).append("</a></td>");
                responseContent.append("<td>").append(numKeys).append("</td>");
                responseContent.append("</tr>");
            }

            responseContent.append("</table></body></html>");
            res.type("text/html");
            res.status(200, "OK");
            return responseContent.toString();
        });
        
        get("/view/:tablename", (req, res) -> {
//        	String fromRow = req.queryParams("fromRow");
//	        if (fromRow==null) {
//	        	fromRow = "";
//	        }
	        final String fromRow;
	        if (req.queryParams("fromRow") != null) {
	            fromRow = req.queryParams("fromRow");
	        } else {
	            fromRow = ""; // Default value if the query parameter is not present
	        }
            // Retrieve the table name from the route parameter
            String tableName = req.params("tablename");
            System.out.println(tableName);
            Map<String, Row> tableRows;
            // Check if the table exists in tableData
            if (tableName.startsWith("pt-")) {
            	tableRows = new HashMap<>();
            	String directoryPath = storageDirectory + "/" + tableName;
                File directory = new File(directoryPath);
                if (!directory.exists()) {
                    res.status(404,"404");
                    return "Table not found";
                }
                for (File file : directory.listFiles()) {
                    if (file.isFile()) {
                    	FileInputStream fi = new FileInputStream(file);
                        Row row = Row.readFrom(fi);
                        tableRows.put(file.getName(), row);
                    }
                }
            	
            }
            else {
	            tableRows = tableData.get(tableName);
	            System.out.println(tableName);
            }
//            if (tableRows != null) {
//            	System.out.println("notnull");
//                // Sort the column names for the table
//                List<String> columnNames = new ArrayList<>(tableRows.values().stream()
//                        .flatMap(row -> row.columns().stream())
//                        .distinct()
//                        .sorted()
//                        .collect(Collectors.toList()));
//
//                // Prepare the HTML response
//                StringBuilder responseContent = new StringBuilder("<html><body>");
//                responseContent.append("<h1>Table: ").append(tableName).append("</h1>");
//                responseContent.append("<table border='1'><tr><th>Row Key</th>");
//
//                // Create table headers from sorted column names
//                for (String columnName : columnNames) {
//                    responseContent.append("<th>").append(columnName).append("</th>");
//                }
//
//                responseContent.append("</tr>");
//
//                // Iterate through rows and display data
//                for (Map.Entry<String, Row> entry : tableRows.entrySet()) {
//                    String rowKey = entry.getKey();
//                    Map<String, byte[]> rowData = entry.getValue().columnsmap();
//
//                    responseContent.append("<tr><td>").append(rowKey).append("</td>");
//
//                    // Iterate through columns and display data or empty if not present
//                    for (String columnName : columnNames) {
//                        byte[] cellBytes = rowData.get(columnName);
//                        String cellValue = (cellBytes != null) ? new String(cellBytes, StandardCharsets.UTF_8) : "";
//                        responseContent.append("<td>").append(cellValue).append("</td>");
//                    }
//
//                    responseContent.append("</tr>");
//                }
//
//                responseContent.append("</table></body></html>");
//                res.type("text/html");
//                res.status(200, "OK");
//                return responseContent.toString();
//            } else {
//                res.status(404, "Not Found");
//                return "Table not found";
            
            if (tableRows != null) {
                System.out.println("notnull");
                
                // Sort the column names for the table
                List<String> columnNames = new ArrayList<>(tableRows.values().stream()
                        .flatMap(row -> row.columns().stream())
                        .distinct()
                        .sorted()
                        .collect(Collectors.toList()));
                
                // Filter and sort the row keys based on the "fromRow" value
                List<String> sortedRowKeys = tableRows.keySet().stream()
                        .filter(rowKey -> rowKey.compareTo(fromRow) >= 0)
                        .sorted()
                        .collect(Collectors.toList());

                // Prepare the HTML response
                StringBuilder responseContent = new StringBuilder("<html><body>");
                responseContent.append("<h1>Table: ").append(tableName).append("</h1>");
                responseContent.append("<table border='1'><tr><th>Row Key</th>");

                // Create table headers from sorted column names
                for (String columnName : columnNames) {
                    responseContent.append("<th>").append(columnName).append("</th>");
                }

                responseContent.append("</tr>");

                int rowCount = 0; // To keep track of displayed rows

                // Iterate through rows and display data
                for (String rowKey : sortedRowKeys) {
                    Map<String, byte[]> rowData = tableRows.get(rowKey).columnsmap();

                    responseContent.append("<tr><td>").append(rowKey).append("</td>");

                    // Iterate through columns and display data or empty if not present
                    for (String columnName : columnNames) {
                        byte[] cellBytes = rowData.get(columnName);
                        String cellValue = (cellBytes != null) ? new String(cellBytes, StandardCharsets.UTF_8) : "";
                        responseContent.append("<td>").append(cellValue).append("</td>");
                    }

                    responseContent.append("</tr>");

                    rowCount++;

                    // Check if there are more rows after displaying 10 rows
                    if (rowCount == 10 && sortedRowKeys.indexOf(rowKey) < sortedRowKeys.size() - 1) {
                        String nextRowKey = sortedRowKeys.get(sortedRowKeys.indexOf(rowKey) + 1);
                        responseContent.append("</table>");
                        // Add a "Next" link with the next key as the value of the fromRow parameter
                        responseContent.append("<a href='/view/").append(tableName)
                                .append("?fromRow=").append(nextRowKey).append("'>Next</a>");
                        break;
                    }
                }

                responseContent.append("</table></body></html>");
                res.type("text/html");
                res.status(200, "OK");
                return responseContent.toString();
            } else {
                res.status(404, "Not Found");
                return "Table not found";
            }
        });
    }
//		get("/view/:tablename", (req, res) -> {
//		    // Retrieve the table name from the route parameter
//		    String tableName = req.params("tablename");
//		
//		    // Check if the table exists in tableData
//		    Map<String, Row> tableRows = tableData.get(tableName);
//		
//		    if (tableRows != null) {
//		        // Sort the row keys
//		        List<String> sortedRowKeys = new ArrayList<>(tableRows.keySet());
//		        Collections.sort(sortedRowKeys);
//		
//		        // Check if the fromRow parameter is present
//		        String fromRow = req.queryParams("fromRow");
//		        int fromIndex = 0;
//		
//		        if (fromRow != null) {
//		            // Find the starting index for pagination
//		            fromIndex = Collections.binarySearch(sortedRowKeys, fromRow);
//		            if (fromIndex < 0) {
//		                fromIndex = -fromIndex - 1;
//		            }
//		        }
//		
//		        // Prepare the HTML response
//		        StringBuilder responseContent = new StringBuilder("<html><body>");
//		        responseContent.append("<h1>Table: ").append(tableName).append("</h1>");
//		        responseContent.append("<table border='1'><tr><th>Row Key</th>");
//		
//		        // Create table headers
//		        Map<String, byte[]> firstRowData = tableRows.get(sortedRowKeys.get(0));
//		        Set<String> columnNames = firstRowData.keySet();
//		
//		        for (String columnName : columnNames) {
//		            responseContent.append("<th>").append(columnName).append("</th>");
//		        }
//		
//		        responseContent.append("</tr>");
//		
//		        // Iterate through rows for pagination
//		        int pageSize = 10;
//		        int endIndex = Math.min(fromIndex + pageSize, sortedRowKeys.size());
//		
//		        for (int i = fromIndex; i < endIndex; i++) {
//		            String rowKey = sortedRowKeys.get(i);
//		            Map<String, byte[]> rowData = tableRows.get(rowKey);
//		
//		            responseContent.append("<tr><td>").append(rowKey).append("</td>");
//		
//		            for (String columnName : columnNames) {
//		                byte[] cellBytes = rowData.get(columnName);
//		                String cellValue = (cellBytes != null) ? new String(cellBytes, StandardCharsets.UTF_8) : "";
//		                responseContent.append("<td>").append(cellValue).append("</td>");
//		            }
//		
//		            responseContent.append("</tr>");
//		        }
//		
//		        responseContent.append("</table>");
//		
//		        // Add pagination links if there are more rows
//		        if (endIndex < sortedRowKeys.size()) {
//		            String nextRow = sortedRowKeys.get(endIndex);
//		            responseContent.append("<a href='/view/").append(tableName)
//		                          .append("?fromRow=").append(nextRow)
//		                          .append("'>Next</a>");
//		        }
//		
//		        responseContent.append("</body></html>");
//		        res.type("text/html");
//		        res.status(200, "OK");
//		        return responseContent.toString();
//		    } else {
//		        res.status(404, "Not Found");
//		        return "Table not found";
//		    }
//		});
//    }
//    @SuppressWarnings("unchecked")
//	private static void updateOnDisk(String tableName, String rowKey, String column, byte[] value) {
//        String directoryPath = storageDirectory + "/" + tableName;
//        File directory = new File(directoryPath);
//        if (!directory.exists()) {
//            directory.mkdir();
//        }
//
//        String fileName = rowKey;
//        File file = new File(directoryPath + "/" + fileName);
//
//        Map<String, String> rowData = null;
//
//        if (!file.exists()) {
//            // File doesn't exist, create a new file with the updated data
//            rowData = new HashMap<>();
//        } else {
//            // File exists, read the existing data
//        	try (FileInputStream fis = new FileInputStream(file);
//        	        BufferedInputStream bis = new BufferedInputStream(fis)) {
//        	        byte[] data = new byte[(int) file.length()];
//        	        bis.read(data);
//        	        // Deserialize the existing data into a Map
//					try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
//					    rowData = (Map<String, String>) ois.readObject();
//					} catch (IOException | ClassNotFoundException e) {
//					    e.printStackTrace();
//					    return;
//					}
//        	} catch (IOException e) {
//        	    e.printStackTrace();
//        	}
//        }
//
//        // Update or add the new data to the Map
//        rowData.put(column, new String(value, StandardCharsets.UTF_8));
//
//        // Serialize the updated Map
//        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
//             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
//            oos.writeObject(rowData);
//
//            // Write the updated data back to the file
//            try (FileOutputStream fos = new FileOutputStream(file);
//                 BufferedOutputStream bos = new BufferedOutputStream(fos)) {
//                bos.write(baos.toByteArray());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
	private static void updateOnDisk(String tableName, String rowkey, String column, byte[] value) throws Exception{
        String directoryPath = storageDirectory + "/" + tableName;
        Row newrow;
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        String fileName = rowkey;
        File file = new File(directoryPath, fileName);
        if (file.exists()) {
        	FileInputStream fi = new FileInputStream(file);
        	newrow=Row.readFrom(fi);
        	putRow(newrow,column,value);
        }
        // Update or add the new data to the Map
        else {
        	newrow= new Row(rowkey);
        	putRow(newrow,column,value);
        }
        try (FileOutputStream fos = new FileOutputStream(directoryPath + "/" + fileName);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
             bos.write(newrow.toByteArray());
         } catch (IOException e) {
             e.printStackTrace();
         }
        // Serialize the updated Map and write it back to the file
        
    }
//    private static void saveToDisk(String tableName, String rowKey, byte[] value) {
//        String directoryPath = storageDirectory + "/"+tableName;
//        File directory = new File(directoryPath);
//        if (!directory.exists()) {
//            directory.mkdir();
//        }
//
//        String fileName =rowKey;
//        try (FileOutputStream fos = new FileOutputStream(directoryPath + "/" + fileName);
//             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
//            bos.write(value);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
    
    private static void putRow( Row row, String column, byte[] value) {
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
