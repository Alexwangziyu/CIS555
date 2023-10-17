package cis5550.kvs;

import static cis5550.webserver.Server.*;

public class Coordinator extends cis5550.generic.Coordinator {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -cp lib/webserver.jar cis5550.kvs.Coordinator <port>");
            return;
        }
        // Server webServer = new Server();
        int port = Integer.parseInt(args[0]);
        port(port);
        registerRoutes();
        get("/", (request, response) -> {
            return "Hello, world!";
        });
    }
}