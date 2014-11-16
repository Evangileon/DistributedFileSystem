import java.io.Serializable;

public class FileChunk implements Comparable<FileChunk>, Serializable {
    public static final int FIXED_SIZE = 8192;
    String realFileName;
    int chunkID;
    int actualLength;

    public FileChunk(String realFileName, int chunkID, int actualLength) {
        this.realFileName = realFileName;
        this.chunkID = chunkID;
        this.actualLength = actualLength;
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
