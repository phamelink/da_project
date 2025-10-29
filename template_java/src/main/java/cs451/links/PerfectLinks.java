package cs451.links;

import cs451.Host;
import cs451.Logger;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PerfectLinks {
    // Retransmission delay
    private static final int RETRANSMISSION_TIMEOUT_MS = 300;

    // --- Components ---
    private final UdpReceiver receiver;
    private final UdpSender sender;
    private final Logger logger;
    private final DatagramSocket datagramSocket;

    private final Host selfProcess;
    // Map: ID -> Host, used for safe host lookups
    private final Map<Integer, Host> processMap;

    // --- State for PL1 (Reliable Delivery) ---
    // Key: receiverId -> Map<seqNum, Message> - Messages sent but not ACKed.
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, Message>> pendingMessages;
    // Key: senderId -> Map<seqNum, Message> - Messages received out-of-order.
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, Message>> receivedBuffer;
    // Key: senderId -> Integer - The sequence number of the last delivered message.
    private final ConcurrentMap<Integer, Integer> lastDeliveredSeqNum;

    private final RetransmissionThread retransmissionThread;


    public PerfectLinks(Host self, List<Host> allHosts, Logger logger) {
        try {
            this.selfProcess = self;
            this.datagramSocket = new DatagramSocket(self.getPort());
            this.logger = logger;
            this.sender = new UdpSender(datagramSocket);

            this.processMap = new ConcurrentHashMap<>();
            this.pendingMessages = new ConcurrentHashMap<>();
            this.receivedBuffer = new ConcurrentHashMap<>();
            this.lastDeliveredSeqNum = new ConcurrentHashMap<>();

            // Initialize maps for all potential communicators
            for (Host h : allHosts) {
                this.processMap.put(h.getId(), h);
                // Senders only track messages to others
                if (h.getId() != self.getId()) {
                    this.pendingMessages.put(h.getId(), new ConcurrentHashMap<>());
                }
                this.receivedBuffer.put(h.getId(), new ConcurrentHashMap<>());
                this.lastDeliveredSeqNum.put(h.getId(), 0);
            }

            // The receiver calls deliverMessage, which acts as the packet dispatcher.
            this.receiver = new UdpReceiver(this.datagramSocket, this::deliverMessage);
            this.retransmissionThread = new RetransmissionThread();

        } catch (SocketException e) {
            System.err.println("Error instantiating PerfectLinks: " + e.getMessage());
            throw new RuntimeException();
        }
    }

    public void start() {
        new Thread(this.receiver, "UdpReceiver").start();
        this.retransmissionThread.start();
    }

    public void stop() {
        this.retransmissionThread.stopRunning();

        // Wait gracefully for the retransmission thread to finish its work
        try {
            this.retransmissionThread.join(RETRANSMISSION_TIMEOUT_MS * 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        this.datagramSocket.close();
        this.receiver.stop();
    }

    // --- PL.Send (Request) ---
    public void send(Host dest, Message msg) {
        if (msg.getType() == Message.Type.DATA) {
            // Only add/log message if it's the *initial* send for this sequence number
            // The redundant check is safe here because this method handles both initial send and retries
            if (!this.pendingMessages.get(dest.getId()).containsKey(msg.getSeqNum())) {
                this.logger.logMessage(msg, Logger.EventType.Sending);
                this.pendingMessages.get(dest.getId()).put(msg.getSeqNum(), msg);
            }
        }
        this.sendPacket(dest, msg);
    }

    public void deliverMessage(Message msg) {
        if (msg.getType() == Message.Type.ACK) {
//            System.out.println("Received ACK msg: " + msg.toString());
            handleAck(msg);
        } else if (msg.getType() == Message.Type.DATA) {
//            System.out.println("Received DATA msg: " + msg.toString());
            handleData(msg);
        } else {
            System.err.println("Received message of unknown type: " + msg.getType());
        }
    }

    private void handleAck(Message ackMsg) {
        int receiverId = ackMsg.getSenderId();
        int acknowledgedSeqNum = ackMsg.getSeqNum();

        // Retrieve the map of messages pending ACK to remove the acknowledged message
        ConcurrentMap<Integer, Message> receiverPendingMap = pendingMessages.get(receiverId);

        if (receiverPendingMap != null) {
            // Atomically remove the message
            receiverPendingMap.remove(acknowledgedSeqNum);
        }
    }

    private void handleData(Message dataMsg) {
        int senderId = dataMsg.getSenderId();
        int seqNum = dataMsg.getSeqNum();

        // 1. Get the host of the sender to send an ACK back
        Host senderHost = processMap.get(senderId);
        if (senderHost == null) {
//            System.err.println("Received DATA message from unknown sender ID: " + senderId);
            return;
        }

        this.receivedBuffer.get(senderId).put(seqNum, dataMsg);

        // 3. Attempt contiguous delivery
        deliverContiguousMessages(senderId);
    }

    private void deliverContiguousMessages(int senderId) {
        ConcurrentMap<Integer, Message> buffer = this.receivedBuffer.get(senderId);
        if (buffer == null) return;

        // Use compute for atomicity of the delivery marker update and contiguous check
        this.lastDeliveredSeqNum.compute(senderId, (id, lastDelivered) -> {
            int currentMax = (lastDelivered != null) ? lastDelivered : 0;
            int nextSeq = currentMax + 1;

            // Loop to deliver all messages that are now contiguous
            while (true) {
                Message nextMessage = buffer.get(nextSeq);

                if (nextMessage != null) {
                    // Found the next contiguous message: deliver and advance the sequence number

                    // PL2 check is implicit here: since we only deliver nextSeq, 
                    // any message with seqNum <= currentMax has been delivered/logged already.
                    this.logger.logMessage(nextMessage, Logger.EventType.Delivery);
                    buffer.remove(nextSeq); // Cleanup the message from the buffer (Memory Reclamation!)

                    nextSeq++;
                } else {
                    // Gap found (or buffer is empty). Stop delivery.
                    break;
                }
            }

            // Return the new high-water mark (or the old one if nothing was delivered)
            return nextSeq - 1;
        });
    }


    private void sendPacket(Host dest, Message msg) {
//        System.out.println("Sending packet to " + dest.getId() + ": " + msg.toString());
        this.sender.send(dest, msg);
    }

    // --- Retransmission Thread for PL1 (Reliable Delivery) ---
    private class RetransmissionThread extends Thread {
        private volatile boolean running = true;

        public void stopRunning() {
            this.running = false;
        }

        @Override
        public void run() {
//            int count = 0;
            while (running) {
//                System.out.println("Retry count " + count);
//                count++;
                try {
                    // Iterate over all receivers that THIS process is sending to
                    for (Map.Entry<Integer, ConcurrentMap<Integer, Message>> entry : pendingMessages.entrySet()) {
                        int receiverId = entry.getKey();
                        ConcurrentMap<Integer, Message> pendingForReceiver = entry.getValue();

                        // Only retransmit if there are pending messages
                        if (!pendingForReceiver.isEmpty()) {
                            Host destHost = processMap.get(receiverId);

                            if (destHost != null) {
                                // Re-send all messages in the pending map for this receiver
                                for (Message msgToRetry : pendingForReceiver.values()) {
                                    // Use the low-level sendPacket method for retries to avoid unnecessary tracking/logging.
//                                    System.out.println("Retrying to send msg: " + msgToRetry.toString());
                                    sendPacket(destHost, msgToRetry);
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
