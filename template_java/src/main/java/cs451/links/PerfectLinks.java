package cs451.links;

import cs451.Host;
import cs451.Logger;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PerfectLinks {
    private UdpReceiver receiver;
    private UdpSender sender;

    private Logger logger;
    private DatagramSocket datagramSocket;

    private Host selfProcess;
    private List<Host> otherProcesses;
    private Map<Integer, Host> processMap;

    // receiverId -> (seq_nr -> Message)
    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Message>> pendingMessages;

    // Contains the msg.toString() content of each delivered message 
    private Set<String> deliveredMessages;

    public PerfectLinks(Host self, List<Host> otherProcesses, Logger logger) {
        try {
            this.otherProcesses = otherProcesses;
            this.processMap = new HashMap<>();
            this.pendingMessages = new ConcurrentHashMap<>();
            for (Host h : this.otherProcesses) {
                this.pendingMessages.put(h.getId(), new ConcurrentHashMap<>());
                this.processMap.put(h.getId(), h);
            }
            this.selfProcess = self;
            this.datagramSocket = new DatagramSocket(self.getPort());
            this.receiver = new UdpReceiver(this.datagramSocket, this::deliverMessage);
            this.sender = new UdpSender(datagramSocket);
            this.logger = logger;

            this.deliveredMessages = new HashSet<>();

        } catch (SocketException e) {
            System.err.println("Error instantiating PerfectLinks: " + e.getMessage());
        }
    }

    public void start() {
        new Thread(this.receiver).start();
    }

    public void stop() {
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
            this.send(this.processMap.get(msg.getSenderId()), msg.createAck(selfProcess.getId()));
            if (!this.deliveredMessages.contains(msg.toString())) {
                this.logger.logMessage(msg, Logger.EventType.Delivery);
                this.deliveredMessages.add(msg.toString());
            }
        }
    }
}
