package cs451.links;

import cs451.Host;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UdpSender {

    private final DatagramSocket socket;

    public UdpSender(DatagramSocket socket) {
        this.socket = socket;
    }

    public boolean send(Host dest, Message msg) {
        try {
            byte[] buf = msg.serialize();
            InetAddress dstAddr = InetAddress.getByName(dest.getIp());
            DatagramPacket packet = new DatagramPacket(buf, buf.length, dstAddr, dest.getPort());
            this.socket.send(packet);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
