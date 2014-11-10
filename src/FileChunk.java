import java.io.Serializable;

public class FileChunk implements Comparable<FileChunk>, Serializable {
    public static final int size = 8192;
    String realFileName;
    int chunkID;

    public FileChunk(String realFileName, int chunkID) {
        this.realFileName = realFileName;
        this.chunkID = chunkID;
    }

    public String getChunkName() {
        return realFileName + "-" + String.format("%08d", chunkID);
    }

    public String toString() {
        return getChunkName();
    }

    @Override
    public int compareTo(FileChunk o) {
        return this.chunkID - o.chunkID;
    }
}
