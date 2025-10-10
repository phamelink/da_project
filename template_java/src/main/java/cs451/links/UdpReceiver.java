package cs451.links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.function.Consumer;

public class UdpReceiver implements Runnable {

    private static final int MAX_UDP_PAYLOAD_SIZE = 1500;

    private final DatagramSocket socket;
    private final Consumer<Message> deliverCallback;
    private boolean running = true;

    public UdpReceiver(DatagramSocket socket, Consumer<Message> deliverCallback) {
        this.socket = socket;
        this.deliverCallback = deliverCallback;
    }

    public void stop() {
        this.running = false;
    }


    @Override
    public void run() {
        while (running && !socket.isClosed()) {
            try {
                byte[] buffer = new byte[MAX_UDP_PAYLOAD_SIZE];
                DatagramPacket datagramPacket = new DatagramPacket(buffer, buffer.length);
                socket.receive(datagramPacket);

                Message msg = Message.deserialize(buffer);

                if (msg != null) {
                    deliverCallback.accept(msg);
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
