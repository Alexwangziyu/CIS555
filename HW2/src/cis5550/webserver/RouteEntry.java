package cis5550.webserver;


public class RouteEntry{
    private String method;
    private String pathPattern;
    private Route handler;

    public RouteEntry(String method, String pathPattern, Route handler) {
        this.method = method;
        this.pathPattern = pathPattern;
        this.handler = handler;
    }

    public String getMethod() {
        return method;
    }

    public String getPathPattern() {
        return pathPattern;
    }

    public Route getHandler() {
        return handler;
    }
}
