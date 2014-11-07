import java.io.Serializable;

/**
 * Created by Jun Yu on 11/6/14.
 */
public class FileChunk implements Comparable<FileChunk>, Serializable {
    public static final int size = 8192;
    String realFileName;
    int chunckID;


    public FileChunk(String realFileName, int chunckID) {
        this.realFileName = realFileName;
        this.chunckID = chunckID;
    }

    public String getChunkName() {
        return realFileName + "-" + String.format("%08d", chunckID);
    }

    @Override
    public int compareTo(FileChunk o) {
        return this.chunckID - o.chunckID;
    }
}
