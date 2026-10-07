package server;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Duplicates manually launched process output to logs while retaining terminal output. */
final class ProcessLog {

    private static final Object WRITE_LOCK = new Object();

    private ProcessLog() {
    }

    static void redirect(String component) throws IOException {
        Path directory = Path.of(System.getProperty("syncchat.logs.dir", "logs"));
        Files.createDirectories(directory);
        FileOutputStream file = new FileOutputStream(directory.resolve(component + ".log").toFile(), true);
        PrintStream consoleOut = System.out;
        PrintStream consoleErr = System.err;
        System.setOut(new PrintStream(new TeeOutputStream(consoleOut, file), true));
        System.setErr(new PrintStream(new TeeOutputStream(consoleErr, file), true));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (WRITE_LOCK) {
                try {
                    file.flush();
                    file.close();
                } catch (IOException ignored) {
                    // Process is exiting; there is no useful recovery action.
                }
            }
        }, "syncchat-log-close-" + component));
    }

    private static final class TeeOutputStream extends OutputStream {
        private final OutputStream console;
        private final OutputStream file;

        private TeeOutputStream(OutputStream console, OutputStream file) {
            this.console = console;
            this.file = file;
        }

        @Override
        public void write(int value) throws IOException {
            synchronized (WRITE_LOCK) {
                console.write(value);
                file.write(value);
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            synchronized (WRITE_LOCK) {
                console.write(bytes, offset, length);
                file.write(bytes, offset, length);
            }
        }

        @Override
        public void flush() throws IOException {
            synchronized (WRITE_LOCK) {
                console.flush();
                file.flush();
            }
        }
    }
}
