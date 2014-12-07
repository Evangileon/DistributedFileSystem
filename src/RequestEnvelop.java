import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

public class RequestEnvelop implements Serializable {
    String cmd;
    String fileName;
    int chunkID;
    ArrayList<String> params;
    char[] data;

    final UUID uuid;

    public RequestEnvelop(String cmd, String fileName) {
        this.cmd = cmd;
        this.fileName = fileName;
        this.params = new ArrayList<>();
        this.data = null;
        this.chunkID = 0;
        this.uuid = UUID.randomUUID();
    }

    public RequestEnvelop(RequestEnvelop request) {
        this(request.cmd, request.fileName, request.chunkID, request.params, request.uuid);
    }

    private RequestEnvelop(String cmd, String fileName, int chunkID, ArrayList<String> params, UUID uuid) {
        this.cmd = cmd;
        this.fileName = fileName;
        this.chunkID = chunkID;
        this.params = params;
        this.uuid = uuid;
    }

    public void addParam(String param) {
        params.add(param);
    }

    public void setData(char[] data) {
        this.data = data;
    }
}
