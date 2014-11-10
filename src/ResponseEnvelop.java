import java.io.Serializable;
import java.util.UUID;

public class ResponseEnvelop implements Serializable {
    RequestEnvelop requestCopy;

    UUID uuid = UUID.randomUUID();

    /**
     * Construct
     * @param request to response
     */
    public ResponseEnvelop(RequestEnvelop request) {
        this.requestCopy = request;
    }
}
