import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

public class RequestEnvelop implements Serializable {
    String cmd;
    String fileName;
    int chunkID;
    ArrayList<String> params;
    char[] data;

    UUID uuid = UUID.randomUUID();

    public RequestEnvelop(String cmd, String fileName) {
        this.cmd = cmd;
        this.fileName = fileName;
        this.params = new ArrayList<>();
        this.data = null;
        this.chunkID = 0;
    }

    public void addParam(String param) {
        params.add(param);
    }

    public void setData(char[] data) {
        this.data = data;
    }
}
