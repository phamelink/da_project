package cs451;

import cs451.links.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public class Logger {
    private final BufferedWriter writer;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed = false;

    public Logger(String outputPath) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(outputPath));
    }

    private void log(String line) {
        lock.lock();
        try {
            if (!closed) {
                writer.write(line);
                writer.newLine();  // Unix line break
            }
        } catch (IOException e) {
            System.err.println("Logger error: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    public void flush() {
        lock.lock();
        try {
            if (!closed) {
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Logger flush error: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Call this in the shutdown
     */
    public void close() {
        lock.lock();
        try {
            if (!closed) {
                writer.flush();
                writer.close();
                closed = true;
            }
        } catch (IOException e) {
            System.err.println("Logger close error: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    public void logMessage(Message msg, EventType eventType) {
        switch (eventType) {
            case Sending:
                this.log(String.format("b %s", msg.getPayload()));
                break;
            case Delivery:
                this.log(String.format("d %s %s", msg.getSenderId(), msg.getPayload()));
                break;
        }
    }


    public enum EventType {
        Sending,
        Delivery
    }

}



