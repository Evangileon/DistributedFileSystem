import java.io.Serializable;
import java.util.LinkedList;
import java.util.UUID;

public class ResponseEnvelop implements Serializable {
    RequestEnvelop requestCopy;
    int error;

    UUID uuid = UUID.randomUUID();

    LinkedList<Integer> chunksToScan;
    LinkedList<Integer> chunksLocation;

    public void setChunksToScan(LinkedList<Integer> chunksToScan) {
        this.chunksToScan = chunksToScan;
    }

    public void setError(int error) {
        this.error = error;
    }

    /**
     * Construct
     *
     * @param request to response
     */
    public ResponseEnvelop(RequestEnvelop request) {
        this.requestCopy = request;
        this.error = 0;
        this.chunksToScan = null;
        this.chunksLocation = null;
    }

    public void setChunksLocation(LinkedList<Integer> chunksLocation) {
        this.chunksLocation = chunksLocation;
    }
}
