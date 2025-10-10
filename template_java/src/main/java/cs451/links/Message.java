package cs451.links;

import java.io.*;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int senderId;
    private final int seqNum;
    private final String payload;


    public Message(int senderId, int seqNum, String payload) {
        this.senderId = senderId;
        this.seqNum = seqNum;
        this.payload = payload;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public String getPayload() {
        return payload;
    }

    public static Message deserialize(byte[] data) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Message) ois.readObject();
        } catch (Exception e) {
            return null;
        }
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objOut = new ObjectOutputStream(byteStream)) {
            objOut.writeObject(this);
        }
        return byteStream.toByteArray();
    }
}
