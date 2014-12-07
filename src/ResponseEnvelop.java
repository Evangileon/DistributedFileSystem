import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.UUID;

public class ResponseEnvelop implements Serializable {
    RequestEnvelop requestCopy;
    int error;

    UUID uuid = UUID.randomUUID();

    ArrayList<String> params;

    LinkedList<Integer> chunksToScan;
    LinkedList<Integer> chunksLocation;

    char[] data;

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
        this.requestCopy = new RequestEnvelop(request);

        this.error = 0;
        this.params = null;
        this.data = null;
        this.chunksToScan = null;
        this.chunksLocation = null;
    }

    public void setChunksLocation(LinkedList<Integer> chunksLocation) {
        this.chunksLocation = chunksLocation;
    }

    public void setData(char[] data) {
        this.data = data;
    }

    public void addParam(String param) {
        if (params == null) {
            params = new ArrayList<>();
        }
        params.add(param);
    }
}
