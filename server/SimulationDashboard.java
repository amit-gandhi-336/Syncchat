package server;

import common.ChatService;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Web dashboard to launch the local demo cluster and view process logs. */
public final class SimulationDashboard {

    private static final int HTTP_PORT = 8080;
    private static final int LOAD_BALANCER_REGISTRY_PORT = 2000;
    private static final int LOAD_BALANCER_EXPORT_PORT = 2200;
    private static final int LOG_LIMIT = 600;
    private static final DateTimeFormatter LOG_TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss");

    private final Map<String, ManagedProcess> processes = new ConcurrentHashMap<>();
    private final ExecutorService httpWorkers = Executors.newCachedThreadPool();
    private final Path projectRoot;
    private HttpServer httpServer;

    private SimulationDashboard(Path projectRoot) {
        this.projectRoot = projectRoot;
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        SimulationDashboard dashboard = new SimulationDashboard(root);
        Runtime.getRuntime().addShutdownHook(new Thread(
            dashboard::stopProcesses,
            "syncchat-dashboard-shutdown"
        ));
        dashboard.startHttpServer();
    }

    private void startHttpServer() throws IOException {
        String bindHost = System.getProperty("syncchat.dashboard.host", "127.0.0.1");
        int bindPort = Integer.getInteger("syncchat.dashboard.port", HTTP_PORT);
        httpServer = HttpServer.create(
            new InetSocketAddress(InetAddress.getByName(bindHost), bindPort),
                0
        );
        httpServer.setExecutor(httpWorkers);
        httpServer.createContext("/", this::serveFrontend);
        httpServer.createContext("/api/status", this::handleStatus);
        httpServer.createContext("/api/logs", this::handleLogs);
        httpServer.createContext("/api/start", this::handleStart);
        httpServer.createContext("/api/stop", this::handleStop);
        httpServer.createContext("/api/register", this::handleRegister);
        httpServer.createContext("/api/users", this::handleUsers);
        httpServer.createContext("/api/send", this::handleSend);
        httpServer.createContext("/api/inbox", this::handleInbox);
        httpServer.createContext("/api/server-status", this::handleServerStatus);
        httpServer.createContext("/api/analytics", this::handleAnalytics);
        httpServer.start();
        System.out.println("SyncChat dashboard: http://" + bindHost + ":" + bindPort);
        if (InetAddress.getByName(bindHost).isLoopbackAddress()) {
            System.out.println("Dashboard is bound to localhost only.");
        } else {
            System.out.println("Dashboard is accessible on the configured network interface.");
            System.out.println("Keep port " + bindPort + " restricted to trusted Tailscale peers.");
        }
    }

    private void serveFrontend(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "GET required"));
            return;
        }
        Path html = projectRoot.resolve("web/index.html");
        if (!Files.isRegularFile(html)) {
            sendText(exchange, 404, "Dashboard page missing: " + html);
            return;
        }
        byte[] body = Files.readAllBytes(html);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        List<Map<String, Object>> state = new ArrayList<>();
        for (String name : List.of("node1", "node2", "node3", "load-balancer")) {
            ManagedProcess process = processes.get(name);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", name);
            item.put("running", process != null && process.process.isAlive());
            item.put("pid", process != null && process.process.isAlive()
                    ? process.process.pid() : null);
            state.add(item);
        }
        sendJson(exchange, 200, Map.of("processes", state));
    }

    private void handleLogs(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        String component = query(exchange).getOrDefault("component", "node1");
        if (!List.of("node1", "node2", "node3", "load-balancer").contains(component)) {
            sendJson(exchange, 400, Map.of("error", "Unknown log component."));
            return;
        }
        ManagedProcess process = processes.get(component);
        if (process != null) {
            sendJson(exchange, 200, Map.of(
                "component", component,
                "running", process.process.isAlive(),
                "lines", process.logSnapshot()
            ));
            return;
        }
        Path logFile = logDirectory().resolve(component + ".log");
        if (!Files.isRegularFile(logFile)) {
            sendJson(exchange, 200, Map.of("component", component, "running", false,
                "lines", List.of("No log file yet. Recompile and start this server from its terminal.")));
            return;
        }
        List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
        int firstLine = Math.max(0, lines.size() - LOG_LIMIT);
        sendJson(exchange, 200, Map.of(
            "component", component,
            "running", true,
            "lines", lines.subList(firstLine, lines.size())
        ));
    }

    private void handleStart(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        Map<String, String> form = form(exchange);
        String consistency = form.getOrDefault("consistency", "STRONG").toUpperCase();
        if (!consistency.equals("STRONG") && !consistency.equals("EVENTUAL")) {
            sendJson(exchange, 400, Map.of("error", "Consistency must be STRONG or EVENTUAL."));
            return;
        }

        synchronized (processes) {
            if (processes.values().stream().anyMatch(item -> item.process.isAlive())) {
                sendJson(exchange, 409, Map.of("error", "A simulation process is already running. Stop it first."));
                return;
            }
            processes.clear();
            try {
                String classpath = absoluteClasspath();
                for (int nodeId = 1; nodeId <= 3; nodeId++) {
                    int registryPort = 2000 + nodeId;
                    long offset = nodeId == 1 ? 0 : (nodeId == 2 ? 100 : -100);
                    boolean primary = nodeId == 1;
                    startProcess("node" + nodeId, List.of(
                            javaExecutable(),
                            "-Djava.rmi.server.hostname=localhost",
                            "-cp", classpath,
                            "server.ClockServer",
                            Integer.toString(nodeId),
                            Integer.toString(registryPort),
                            Long.toString(offset),
                            Boolean.toString(primary),
                            consistency
                    ));
                }
                startProcess("load-balancer", List.of(
                        javaExecutable(),
                        "-Djava.rmi.server.hostname=localhost",
                        "-cp", classpath,
                        "server.LoadBalancer",
                        Integer.toString(LOAD_BALANCER_REGISTRY_PORT),
                        Integer.toString(LOAD_BALANCER_EXPORT_PORT),
                        "1@localhost@2001,2@localhost@2002,3@localhost@2003"
                ));
                sendJson(exchange, 200, Map.of(
                        "message", "Started three servers and the load balancer in " + consistency + " mode."
                ));
            } catch (Exception e) {
                stopProcesses();
                sendJson(exchange, 500, Map.of("error", "Could not start simulation: " + e.getMessage()));
            }
        }
    }

    private void handleStop(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        stopProcesses();
        sendJson(exchange, 200, Map.of("message", "Simulation stopped."));
    }

    private synchronized void startProcess(String name, List<String> command) throws IOException {
        ManagedProcess old = processes.get(name);
        if (old != null && old.process.isAlive()) {
            throw new IllegalStateException(name + " is already running");
        }
        ProcessBuilder builder = new ProcessBuilder(command)
                .directory(projectRoot.toFile())
                .redirectErrorStream(true);
        Process process = builder.start();
        ManagedProcess managed = new ManagedProcess(name, process);
        processes.put(name, managed);
        managed.captureOutput();
        managed.addLog("Started: " + String.join(" ", command));
    }

    private void stopProcesses() {
        synchronized (processes) {
            List<String> stopOrder = List.of("load-balancer", "node3", "node2", "node1");
            for (String name : stopOrder) {
                ManagedProcess managed = processes.get(name);
                if (managed == null || !managed.process.isAlive()) continue;
                managed.addLog("Stopping process.");
                managed.process.destroy();
            }
            for (String name : stopOrder) {
                ManagedProcess managed = processes.get(name);
                if (managed == null || !managed.process.isAlive()) continue;
                try {
                    if (!managed.process.waitFor(1500, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        managed.process.destroyForcibly();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    managed.process.destroyForcibly();
                }
            }
        }
    }

    private void handleRegister(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            String username = required(form(exchange), "username");
            boolean created = chatService().registerUser(username);
            sendJson(exchange, 200, Map.of("registered", created,
                    "message", created ? "User registered." : "Username already exists."));
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

    private void handleSend(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            Map<String, String> values = form(exchange);
            chatService().sendMessage(
                    required(values, "sender"),
                    required(values, "receiver"),
                    required(values, "content")
            );
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
                    .map(message -> Map.of("text", message.toString()))
                    .toList();
            sendJson(exchange, 200, Map.of("messages", items));
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

    private ChatService chatService() throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost", LOAD_BALANCER_REGISTRY_PORT);
        return (ChatService) registry.lookup("ChatService");
    }

    private Path logDirectory() {
        Path configured = Path.of(System.getProperty("syncchat.logs.dir", "logs"));
        return (configured.isAbsolute() ? configured : projectRoot.resolve(configured)).normalize();
    }

    private boolean requireMethod(HttpExchange exchange, String method) throws IOException {
        if (method.equals(exchange.getRequestMethod())) return true;
        sendJson(exchange, 405, Map.of("error", method + " required"));
        return false;
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing " + key + ".");
        }
        return value.trim();
    }

    private static Map<String, String> form(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        return parsePairs(body);
    }

    private static Map<String, String> query(HttpExchange exchange) {
        return parsePairs(exchange.getRequestURI().getRawQuery());
    }

    private static Map<String, String> parsePairs(String encoded) {
        Map<String, String> values = new LinkedHashMap<>();
        if (encoded == null || encoded.isEmpty()) return values;
        for (String pair : encoded.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            values.put(key, value);
        }
        return values;
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name", "").toLowerCase().contains("win")
                ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable).toString();
    }

    private String absoluteClasspath() {
        String[] entries = System.getProperty("java.class.path").split(
                java.util.regex.Pattern.quote(System.getProperty("path.separator"))
        );
        List<String> absolute = new ArrayList<>();
        for (String entry : entries) {
            Path path = Path.of(entry);
            absolute.add((path.isAbsolute() ? path : projectRoot.resolve(path))
                    .normalize().toString());
        }
        return String.join(System.getProperty("path.separator"), absolute);
    }

    private static String message(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) cause = cause.getCause();
        String detail = cause.getMessage();
        return detail == null || detail.isBlank() ? error.toString() : detail;
    }

    private static void sendJson(HttpExchange exchange, int status, Object value)
            throws IOException {
        byte[] body = toJson(value).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static void sendText(HttpExchange exchange, int status, String value)
            throws IOException {
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
            map.forEach((key, val) -> entries.add(toJson(String.valueOf(key)) + ":" + toJson(val)));
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
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.toString();
    }

    private static final class ManagedProcess {
        private final String name;
        private final Process process;
        private final Deque<String> lines = new ArrayDeque<>();

        private ManagedProcess(String name, Process process) {
            this.name = name;
            this.process = process;
        }

        private void captureOutput() {
            Thread reader = new Thread(() -> {
                try (InputStream stream = process.getInputStream();
                     BufferedReader input = new BufferedReader(
                             new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = input.readLine()) != null) addLog(line);
                } catch (IOException e) {
                    addLog("Log capture stopped: " + e.getMessage());
                }
            }, "syncchat-log-" + name);
            reader.setDaemon(true);
            reader.start();
        }

        private synchronized void addLog(String line) {
            lines.addLast("[" + LocalTime.now().format(LOG_TIME) + "] " + line);
            while (lines.size() > LOG_LIMIT) lines.removeFirst();
        }

        private synchronized List<String> logSnapshot() {
            return new ArrayList<>(lines);
        }
    }
}
