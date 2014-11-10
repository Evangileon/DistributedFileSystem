import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class RequestEnvelop implements Serializable {
    String cmd;
    String fileName;
    ArrayList<String> params;

    final UUID uuid = UUID.randomUUID();

    public RequestEnvelop(String cmd, String fileName) {
        this.cmd = cmd;
        this.fileName = fileName;
        this.params = new ArrayList<>();
    }

    public void addParam(String param) {
        params.add(param);
    }

    public void addParam(String[] params) {
        this.params.addAll(Arrays.asList(params));
    }
}
