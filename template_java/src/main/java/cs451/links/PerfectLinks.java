package cs451.links;

import cs451.Host;
import cs451.Logger;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class PerfectLinks {
    private static final int RETRANSMISSION_TIMEOUT_MS = 100;
    private UdpReceiver receiver;
    private UdpSender sender;

    private Logger logger;
    private DatagramSocket datagramSocket;

    private Host selfProcess;
    private Map<Integer, Host> otherProcesses;

    // receiverId -> (seq_nr -> Message)
    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Message>> pendingMessages;

    // Contains the msg.toString() content of each delivered message 
    private ConcurrentSkipListSet<String> deliveredMessages;

    private RetransmissionThread retransmissionThread;

    public PerfectLinks(Host self, List<Host> otherProcesses, Logger logger) {
        try {
            this.otherProcesses = new HashMap<>();
            this.pendingMessages = new ConcurrentHashMap<>();
            for (Host h : otherProcesses) {
                this.pendingMessages.put(h.getId(), new ConcurrentHashMap<>());
                this.otherProcesses.put(h.getId(), h);
            }
            this.selfProcess = self;
            this.datagramSocket = new DatagramSocket(self.getPort());
            this.receiver = new UdpReceiver(this.datagramSocket, this::deliverMessage);
            this.sender = new UdpSender(datagramSocket);
            this.logger = logger;

            this.deliveredMessages = new ConcurrentSkipListSet<>();

            this.retransmissionThread = new RetransmissionThread();

        } catch (SocketException e) {
            System.err.println("Error instantiating PerfectLinks: " + e.getMessage());
        }
    }

    public void start() {
        new Thread(this.receiver).start();
        this.retransmissionThread.start();
    }

    public void stop() {
        this.retransmissionThread.stopRunning();
        this.datagramSocket.close();
        this.receiver.stop();
    }

    public void send(Host dest, Message msg) {
        if (msg.getType().equals(Message.Type.DATA)) {
            if (!this.pendingMessages.get(dest.getId()).containsKey(msg.getSeqNum())) {
                this.logger.logMessage(msg, Logger.EventType.Sending);
                this.pendingMessages.get(dest.getId()).put(msg.getSeqNum(), msg);
            }
        }
        this.sender.send(dest, msg);
    }

    public void deliverMessage(Message msg) {
        System.out.println(msg.getPayload());
        if (msg.getType().equals(Message.Type.ACK)) {
            this.pendingMessages.get(msg.getSenderId()).remove(msg.getSeqNum());
        } else {
            this.send(this.otherProcesses.get(msg.getSenderId()), msg.createAck(selfProcess.getId()));
            if (!this.deliveredMessages.contains(msg.toString())) {
                this.logger.logMessage(msg, Logger.EventType.Delivery);
                this.deliveredMessages.add(msg.toString());
            }
        }
    }

    private class RetransmissionThread extends Thread {
        private volatile boolean running = true;

        public void stopRunning() {
            this.running = false;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    // Iterate over all receivers (PIDs 1 to N)
                    for (Map.Entry<Integer, ConcurrentHashMap<Integer, Message>> entry : pendingMessages.entrySet()) {
                        int receiverId = entry.getKey();
                        ConcurrentMap<Integer, Message> pendingForReceiver = entry.getValue();

                        // Only retransmit if there are pending messages
                        if (!pendingForReceiver.isEmpty()) {
                            Host destHost = otherProcesses.get(receiverId);
                            if (destHost != null) {
                                // Re-send all messages in the pending map for this receiver
                                for (Message msgToRetry : pendingForReceiver.values()) {
                                    // Use the public send method, but bypass the DATA logging/pending add logic
                                    sender.send(destHost, msgToRetry);
                                }
                            }
                        }
                    }

                    // Wait for the next retransmission attempt
                    Thread.sleep(RETRANSMISSION_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }
    }
}
