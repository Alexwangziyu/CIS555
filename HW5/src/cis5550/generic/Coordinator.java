package cis5550.generic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.RowFilter.Entry;

import static cis5550.webserver.Server.*;

public class Coordinator {
    private static Map<String, String> workerTable = new HashMap<>();
    private static Map<String, Long> workertime = new HashMap<>();

    public static List<String> getWorkers() {
        List<String> workerList = new ArrayList<>();

        // Iterate through the workerTable and add ip:port strings to the list
        for (Map.Entry<String, String> entry : workerTable.entrySet()) {
            if (workertime.get(entry.getKey()) > System.currentTimeMillis() - 15000) {
                workerList.add(entry.getKey() + "," + entry.getValue());
            }
        }
        return workerList;
    }

    public Map<String, String> workerTable() {
        // Implement logic to return worker table (dummy values for now)
        return workerTable;
    }

    public static void registerRoutes() {
        get("/ping", (req, res) -> {
            // Handle /ping route, add or update worker entry
            String workerId = req.queryParams("id");
            String port = req.queryParams("port");
            System.out.println(workerId + " " + port);
            if (workerId == null || port == null) {
                res.status(400, "400");
                return res;
            }
            workerTable.put(workerId, req.ip() + ":" + port);
            long currentTime = System.currentTimeMillis();
            workertime.put(workerId, currentTime);
            System.out.println(req.ip() + " " + port);
            res.status(200, "OK");
            return "OK";
        });
        get("/workers", (req, res) -> {
            // Get the number of active workers
            List<String> workerList = getWorkers();
            int k = workerList.size();
            // Prepare the response content
            StringBuilder responseContent = new StringBuilder();
            responseContent.append(k).append("\n"); // First line with k
            // Iterate through the workerTable and append worker information
            for (String workerInfo : workerList) {
                responseContent.append(workerInfo).append("\n");
            }
            res.type("text/plain");
            res.status(200, "OK");
            // Return the response content
            return responseContent;
        });
        get("/", (req, res) -> {
            StringBuilder responseContent = new StringBuilder();
            responseContent.append("<html><body>");
            responseContent.append("<h1>Active Workers</h1>");
            responseContent.append("<table>");
            responseContent.append("<thead><tr><th>ID</th><th>IP</th><th>Port</th></tr></thead>");
            responseContent.append("<tbody>");

            for (Map.Entry<String, String> entry : workerTable.entrySet()) {
                String workerId = entry.getKey();
                String workerInfo = entry.getValue();
                String[] parts = workerInfo.split(":");
                String ip = parts[0];
                String port = parts[1];
                if (workertime.get(workerId) > System.currentTimeMillis() - 15000) {

                    responseContent.append("<tr>");
                    responseContent.append("<td>").append(workerId).append("</td>");
                    responseContent.append("<td>").append(ip).append("</td>");
                    responseContent.append("<td>").append(port).append("</td>");
                    responseContent.append("<td><a href=\"http://").append(ip).append(":").append(port).append("/\">")
                            .append(ip).append(":").append(port).append("</a></td>");
                    responseContent.append("</tr>");
                }
            }

            responseContent.append("</tbody>");
            responseContent.append("</table>");
            responseContent.append("</body></html>");

            res.type("text/html");
            return responseContent.toString();
        });

    }

}
