package server;

import common.ChatService;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * Personal HTTP dashboard each cluster member runs alongside their own node.
 *
 * <p>Each person:
 * <ol>
 *   <li>Starts their ClockServer node manually (writes logs to logs/nodeN.log via ProcessLog).
 *   <li>Starts the LoadBalancer on one machine.
 *   <li>Runs this ClientDashboard on their machine.
 *   <li>Opens http://localhost:8080 (or their Tailscale IP) in the browser.
 * </ol>
 *
 * <p>Usage:
 * <pre>
 *   java --add-modules jdk.httpserver \
 *        -Dsyncchat.node.id=1 \
 *        -Dsyncchat.lb.host=100.68.59.4 \
 *        -Dsyncchat.lb.port=2000 \
 *        -Dsyncchat.dashboard.host=0.0.0.0 \
 *        -cp out server.ClientDashboard
 * </pre>
 */
public final class ClientDashboard {

    private static final int    DEFAULT_HTTP_PORT = 8080;
    private static final int    DEFAULT_LB_PORT   = 2000;
    private static final String DEFAULT_LB_HOST   = "localhost";
    private static final int    LOG_TAIL_LINES    = 600;

    /** RMI registry host of the load balancer. */
    private final String lbHost;
    /** RMI registry port of the load balancer. */
    private final int    lbPort;
    /** Node ID whose log file we primarily expose (1, 2, or 3). */
    private final int    nodeId;
    /** Root directory used to locate log files. */
    private final Path   logsDir;

    private HttpServer httpServer;

    private ClientDashboard(String lbHost, int lbPort, int nodeId, Path logsDir) {
        this.lbHost  = lbHost;
        this.lbPort  = lbPort;
        this.nodeId  = nodeId;
        this.logsDir = logsDir;
    }

    // ──────────────────────────────────────────────────────────────────
    // ENTRY POINT
    // ──────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String lbHost   = System.getProperty("syncchat.lb.host",   DEFAULT_LB_HOST);
        int    lbPort   = Integer.getInteger("syncchat.lb.port",   DEFAULT_LB_PORT);
        int    nodeId   = Integer.getInteger("syncchat.node.id",   1);
        String logsPath = System.getProperty("syncchat.logs.dir",  "logs");
        String bindHost = System.getProperty("syncchat.dashboard.host", "127.0.0.1");
        int    httpPort = Integer.getInteger("syncchat.dashboard.port", DEFAULT_HTTP_PORT);

        Path logsDir = Path.of(logsPath).toAbsolutePath().normalize();

        ClientDashboard dashboard = new ClientDashboard(lbHost, lbPort, nodeId, logsDir);
        Runtime.getRuntime().addShutdownHook(new Thread(dashboard::stop, "client-dashboard-shutdown"));
        dashboard.start(bindHost, httpPort);
    }

    // ──────────────────────────────────────────────────────────────────
    // HTTP SERVER
    // ──────────────────────────────────────────────────────────────────

    private void start(String bindHost, int port) throws IOException {
        httpServer = HttpServer.create(
                new InetSocketAddress(InetAddress.getByName(bindHost), port), 0);
        httpServer.setExecutor(Executors.newCachedThreadPool());

        // Serve the web frontend
        httpServer.createContext("/",                    this::serveFrontend);
        // Chat API (proxied through this dashboard to the load balancer via RMI)
        httpServer.createContext("/api/register",        this::handleRegister);
        httpServer.createContext("/api/send",            this::handleSend);
        httpServer.createContext("/api/inbox",           this::handleInbox);
        httpServer.createContext("/api/users",           this::handleUsers);
        httpServer.createContext("/api/server-status",   this::handleServerStatus);
        httpServer.createContext("/api/analytics",       this::handleAnalytics);
        // Log streaming (reads local log files)
        httpServer.createContext("/api/logs",            this::handleLogs);
        httpServer.createContext("/api/node-id",         this::handleNodeId);

        httpServer.start();

        System.out.println("==============================================");
        System.out.println(" SyncChat Client Dashboard");
        System.out.println("==============================================");
        System.out.println(" Dashboard URL : http://" + bindHost + ":" + port);
        System.out.println(" My Node ID    : " + nodeId);
        System.out.println(" Load Balancer : " + lbHost + ":" + lbPort);
        System.out.println(" Logs dir      : " + logsDir);
        System.out.println("==============================================");
    }

    private void stop() {
        if (httpServer != null) httpServer.stop(0);
    }

    // ──────────────────────────────────────────────────────────────────
    // FRONTEND
    // ──────────────────────────────────────────────────────────────────

    private void serveFrontend(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "GET required"));
            return;
        }
        // Walk up from the class file location to find the project root containing web/index.html
        Path html = findWebRoot().resolve("web/index.html");
        if (!Files.isRegularFile(html)) {
            sendText(exchange, 404, "Cannot locate web/index.html. Run from the project root.");
            return;
        }
        byte[] body = Files.readAllBytes(html);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    /** Resolves the project root from the running process working directory. */
    private static Path findWebRoot() {
        return Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    }

    // ──────────────────────────────────────────────────────────────────
    // CHAT API — proxied to the load balancer via RMI
    // ──────────────────────────────────────────────────────────────────

    private void handleRegister(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            String username = required(form(exchange), "username");
            boolean created = chatService().registerUser(username);
            sendJson(exchange, 200, Map.of(
                    "registered", created,
                    "message", created ? "User registered." : "Username already exists."));
        } catch (Exception e) {
            sendJson(exchange, 502, Map.of("error", message(e)));
        }
    }

    private void handleSend(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            Map<String, String> values = form(exchange);
            chatService().sendMessage(
                    required(values, "sender"),
                    required(values, "receiver"),
                    required(values, "content"));
            sendJson(exchange, 200, Map.of("message", "Message sent."));
        } catch (Exception e) {
            sendJson(exchange, 502, Map.of("error", message(e)));
        }
    }

    private void handleInbox(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        try {
            String username = required(query(exchange), "username");
            List<Map<String, String>> items = chatService().getMessages(username).stream()
                    .map(msg -> Map.of("text", msg.toString()))
                    .toList();
            sendJson(exchange, 200, Map.of("messages", items));
        } catch (Exception e) {
            sendJson(exchange, 502, Map.of("error", message(e)));
        }
    }

    private void handleUsers(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        try {
            sendJson(exchange, 200, Map.of("users", chatService().getRegisteredUsers()));
        } catch (Exception e) {
            sendJson(exchange, 502, Map.of("error", message(e)));
        }
    }

    private void handleServerStatus(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        try {
            sendJson(exchange, 200, Map.of("status", chatService().getServerStatus()));
        } catch (Exception e) {
            sendJson(exchange, 502, Map.of("error", message(e)));
        }
    }

    private void handleAnalytics(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        try {
            sendJson(exchange, 200, Map.of("counts", chatService().getMessageCountsBySender()));
        } catch (Exception e) {
            sendJson(exchange, 502, Map.of("error", message(e)));
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // LOG STREAMING — reads from local log files written by ProcessLog
    // ──────────────────────────────────────────────────────────────────

    /**
     * GET /api/logs?component=node1|node2|node3|load-balancer
     *
     * Returns the tail of the corresponding log file as a JSON lines array.
     * Falls back gracefully when the file does not exist yet.
     */
    private void handleLogs(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        String component = query(exchange).getOrDefault("component", "node" + nodeId);
        // Normalise component name → file name
        String fileName = component.equals("load-balancer") ? "load-balancer.log"
                        : component + ".log";
        Path logFile = logsDir.resolve(fileName);
        List<String> lines = tailFile(logFile, LOG_TAIL_LINES);
        sendJson(exchange, 200, Map.of(
                "component", component,
                "file",      logFile.toString(),
                "exists",    Files.exists(logFile),
                "lines",     lines));
    }

    /** Returns which node ID this dashboard is attached to. */
    private void handleNodeId(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        sendJson(exchange, 200, Map.of("nodeId", nodeId, "lbHost", lbHost, "lbPort", lbPort));
    }

    /**
     * Reads the last {@code maxLines} lines from a file efficiently.
     * Returns an empty list when the file does not exist.
     */
    private static List<String> tailFile(Path file, int maxLines) {
        if (!Files.exists(file)) {
            return List.of("Log file not found: " + file
                    + " — start the node server first.");
        }
        Deque<String> tail = new ArrayDeque<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                tail.addLast(line);
                if (tail.size() > maxLines) tail.removeFirst();
            }
        } catch (IOException e) {
            tail.addLast("[Error reading log: " + e.getMessage() + "]");
        }
        return new ArrayList<>(tail);
    }

    // ──────────────────────────────────────────────────────────────────
    // RMI LOAD BALANCER CONNECTION
    // ──────────────────────────────────────────────────────────────────

    private ChatService chatService() throws Exception {
        Registry registry = LocateRegistry.getRegistry(lbHost, lbPort);
        return (ChatService) registry.lookup("ChatService");
    }

    // ──────────────────────────────────────────────────────────────────
    // HTTP UTILITIES
    // ──────────────────────────────────────────────────────────────────

    private boolean requireMethod(HttpExchange exchange, String method) throws IOException {
        if (method.equals(exchange.getRequestMethod())) return true;
        sendJson(exchange, 405, Map.of("error", method + " required"));
        return false;
    }

    private static String required(Map<String, String> values, String key) {
        String v = values.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing: " + key);
        return v.trim();
    }

    private static Map<String, String> form(HttpExchange exchange) throws IOException {
        return parsePairs(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
    }

    private static Map<String, String> query(HttpExchange exchange) {
        return parsePairs(exchange.getRequestURI().getRawQuery());
    }

    private static Map<String, String> parsePairs(String encoded) {
        Map<String, String> map = new LinkedHashMap<>();
        if (encoded == null || encoded.isEmpty()) return map;
        for (String pair : encoded.split("&")) {
            String[] parts = pair.split("=", 2);
            String key   = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            map.put(key, value);
        }
        return map;
    }

    private static String message(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) cause = cause.getCause();
        String msg = cause.getMessage();
        return (msg == null || msg.isBlank()) ? t.toString() : msg;
    }

    private static void sendJson(HttpExchange exchange, int status, Object value) throws IOException {
        byte[] body = toJson(value).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static void sendText(HttpExchange exchange, int status, String value) throws IOException {
        byte[] body = value.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static String toJson(Object value) {
        if (value == null) return "null";
        if (value instanceof String text) return "\"" + jsonEscape(text) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) {
            List<String> entries = new ArrayList<>();
            map.forEach((k, v) -> entries.add(toJson(String.valueOf(k)) + ":" + toJson(v)));
            return "{" + String.join(",", entries) + "}";
        }
        if (value instanceof Iterable<?> iterable) {
            List<String> entries = new ArrayList<>();
            for (Object item : iterable) entries.add(toJson(item));
            return "[" + String.join(",", entries) + "]";
        }
        return toJson(value.toString());
    }

    private static String jsonEscape(String text) {
        StringBuilder out = new StringBuilder(text.length() + 16);
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"'  -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default   -> { if (c < 0x20) out.append(String.format("\\u%04x", (int) c)); else out.append(c); }
            }
        }
        return out.toString();
    }
}
