package cs451;

import cs451.links.Message;
import cs451.links.PerfectLinks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    private static Logger logger;
    private static PerfectLinks pl;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        if (pl != null) {
            pl.stop();
        }
        //write/flush output file if necessary
        System.out.println("Writing output.");
        if (logger != null) {
            logger.close();
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");

        logger = new Logger(parser.output());

        // Parse config: "m i"
        int m, receiverId;
        try (BufferedReader br = new BufferedReader(new FileReader(parser.config()))) {
            String[] parts = br.readLine().trim().split("\\s+");
            m = Integer.parseInt(parts[0]);
            receiverId = Integer.parseInt(parts[1]);
        }

        Host self = parser.hosts().get(parser.myId() - 1);
        pl = new PerfectLinks(self, parser.hosts(), logger);

        pl.start(); // start receiver thread

        // --- Role decision ---
        if (parser.myId() != receiverId) {
            // Sender
            Host receiverHost = parser.hosts().get(receiverId - 1);

            for (int seq = 1; seq <= m; seq++) {
                Message msg = new Message(parser.myId(), seq, String.valueOf(seq));
                pl.send(receiverHost, msg);
            }

            System.out.println("All messages sent.");
        } else {
            // Receiver: does nothing except deliver in callback
            System.out.println("Running as receiver (id = " + receiverId + ")");
        }

        System.out.println("Broadcasting and delivering messages...\n");

        // Wait forever (until SIGINT/SIGTERM)
        while (true) {
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
