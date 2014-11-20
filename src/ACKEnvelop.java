import java.io.Serializable;
import java.util.UUID;

/**
 * @author Jun Yu
 */


public class ACKEnvelop implements Serializable {
    public static final int CLIENT_ACK = 1;
    public static final int FILE_SERVER_ACK = 2;
    public static final int META_SERVER_ACK = 3;

    int type;
    int id;
    long ackNo;
    private static long ackSerie = 0;
    final UUID uuid = UUID.randomUUID();

    FileInfo fileInfo = null;

    private ACKEnvelop(int type) {
        this.type = type;
    }

    private static synchronized long newAck() {
        return (++ackSerie);
    }

    public static ACKEnvelop clientAck() {
        ACKEnvelop ack = new ACKEnvelop(CLIENT_ACK);
        ack.ackNo = newAck();
        return ack;
    }

    public static ACKEnvelop fileServerAck(int id, FileInfo fileInfo) {
        ACKEnvelop ack = new ACKEnvelop(FILE_SERVER_ACK);
        ack.id = id;
        ack.fileInfo = fileInfo;
        ack.ackNo = newAck();
        return ack;
    }

    public static ACKEnvelop metaServerAck(long ackNo) {
        ACKEnvelop ack = new ACKEnvelop(META_SERVER_ACK);
        ack.ackNo = ackNo;
        return ack;
    }
}
