package cs451.links;

import cs451.Host;
import cs451.Logger;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;

public class PerfectLinks {
    private UdpReceiver receiver;
    private UdpSender sender;

    private Logger logger;
    private DatagramSocket datagramSocket;

    public PerfectLinks(Host self, List<Host> otherProcesses, Logger logger) {
        try {
            this.datagramSocket = new DatagramSocket(self.getPort());
            this.receiver = new UdpReceiver(this.datagramSocket, this::deliverMessage);
            this.sender = new UdpSender(datagramSocket);
            this.logger = logger;
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
        this.sender.send(dest, msg);
        this.logger.logMessage(msg, Logger.EventType.Sending);
    }

    public void deliverMessage(Message msg) {
        System.out.println(msg.getPayload());
        this.logger.logMessage(msg, Logger.EventType.Delivery);
    }
}
