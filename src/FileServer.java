/**
 * @author Jun Yu on 11/5/14.
 */

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;


public class FileServer {

    int id;
    String hostname;

    InetAddress fileServerAddress;
    int receiveMetaFilePort; // receive from meta server
    int requestFilePort; // request from client
    int fileServerExchangePort; // the port this server opens to other file servers

    MetaServer metaServer;

    final FileInfo fileInfo = new FileInfo();
    String storageDir;

    // socket to send heartbeat
    Socket heartbeatSock;
    // socket to listen to request
    ServerSocket requestSock;

    HashMap<Integer, FileServer> allFileServerList;

    //public static final int CHUNK_LENGTH = 8192;

    public FileServer() {

    }

    /**
     * Parse XML to acquire hostname, ports of process
     *
     * @param filename xml
     */
    private void parseXML(String filename) {
        allFileServerList = new HashMap<>();

        File fXmlFile = new File(filename);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;

        try {
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
            //Node root = doc.getDocumentElement();
            if (!doc.hasChildNodes()) {
                System.exit(0);
            }
            doc.normalize();

            // config for meta server
            Node metaServerNode = doc.getElementsByTagName("metaServer").item(0);
            metaServer = new MetaServer(metaServerNode);
            // config for this file server
            String hostName = InetAddress.getLocalHost().getHostName();
            XPathFactory xPathFactory = XPathFactory.newInstance();
            XPath xPath = xPathFactory.newXPath();
            Node thisFileServerNode = getFileServerNodeWithHostname(doc, xPath, hostName);
            // TODO XPath to find proper file server config for this
            //Node thisFileServerNode = doc.getElementsByTagName("fileServer").item(0);
            parseXMLToConfigFileServer(thisFileServerNode);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    private static Node getFileServerNodeWithHostname(Document doc, XPath xPath, String hostname) {
        NodeList nodes = null;
        try {
            XPathExpression expr = xPath.compile(String.format("/configs/fileServers/fileServer[hostname='%s']", hostname));
            nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);


        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        if (nodes == null || nodes.getLength() == 0) {
            return null;
        }

        return nodes.item(0);
    }

    public FileServer(int id, Node serverNode) {
        this.id = id;
        parseXMLToConfigFileServer(serverNode);
    }

    public void resolveAddress() {
        if (hostname == null) {
            return;
        }

        try {
            fileServerAddress = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void parseXMLToConfigFileServer(Node serverNode) {

        NodeList serverConfig = serverNode.getChildNodes();

        for (int j = 0; j < serverConfig.getLength(); j++) {
            Node oneConfig = serverConfig.item(j);
            if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            String nodeName = oneConfig.getNodeName();
            String text = oneConfig.getTextContent();
            if (nodeName.equals("id")) {
                this.id = Integer.valueOf(text);
            }
            if (nodeName.equals("hostname")) {
                this.hostname = text;
            }
            if (nodeName.equals("receiveMetaFilePort")) {
                this.receiveMetaFilePort = Integer.parseInt(text);
            }
            if (nodeName.equals("requestFilePort")) {
                this.requestFilePort = Integer.parseInt(text);
            }
            if (nodeName.equals("storageDir")) {
                this.storageDir = text;
            }
        }
    }

    /**
     * Create a new thread to send file meta data along with heartbeat message
     */
    private void heartbeatToMetaServer() {
        System.out.println("Meta server heartbeat: " + metaServer.hostname);
        System.out.println("Meta server heartbeat: " + metaServer.metaServerAddress.toString() + ":" + metaServer.receiveHeartbeatPort);

        while (true) {
            try {
                heartbeatSock = new Socket(metaServer.metaServerAddress, metaServer.receiveHeartbeatPort);

                //##############################################################
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Enter heartbeat send loop");
                        ObjectOutputStream output;
                        while (true) {
                            try {
                                output = new ObjectOutputStream(heartbeatSock.getOutputStream());
                                output.writeObject(fileInfo);
                                output.flush();

                            } catch (IOException e) {
                                System.out.println(heartbeatSock.getRemoteSocketAddress().toString());
                                e.printStackTrace();
                                break;
                            }
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.println("Exit heartbeat send loop");
                    }
                });
                thread.setDaemon(true);
                thread.start();
                //##############################################################

                try {
                    thread.join();
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }

                System.out.println("heartbeat reconnect");

            } catch (IOException e) {
                //System.out.println(heartbeatSock.getInetAddress().toString());
                e.printStackTrace();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    /**
     * Create a new thread to keep sending heartbeat
     */
    private void prepareToSendHeartbeat() {
        Thread threadHeartbeat = new Thread(new Runnable() {
            @Override
            public void run() {
                heartbeatToMetaServer();
            }
        });

        threadHeartbeat.setDaemon(true);
        threadHeartbeat.start();
    }

    /**
     * Create a thread to listen to request from clients or meta server
     */
    private void prepareToReceiveRequest() {
        try {
            requestSock = new ServerSocket(requestFilePort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Request listen port: " + requestSock.getLocalPort());

        Thread requestHandleRequest = new Thread(new Runnable() {
            @Override
            public void run() {
                // note that meta server can also be requester
                while (true) {
                    try {
                        Socket clientSock = requestSock.accept();
                        System.out.println("Receive request from: " + clientSock.getInetAddress().toString());

                        ResponseRequestEntity responseRequestEntity = new ResponseRequestEntity(clientSock);
                        Thread threadResponse = new Thread(responseRequestEntity);
                        threadResponse.setDaemon(true);
                        threadResponse.run();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        requestHandleRequest.setDaemon(true);
        requestHandleRequest.start();
    }

    /**
     * Thread to handle request
     */
    class ResponseRequestEntity implements Runnable {

        Socket clientSock;

        public ResponseRequestEntity(Socket clientSock) {
            this.clientSock = clientSock;
        }

        @Override
        public void run() {

            try {
                ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());
                RequestEnvelop request = (RequestEnvelop) input.readObject();

                ResponseEnvelop response = new ResponseEnvelop(request);

                String cmd = request.cmd;
                String fileName = request.fileName;
                int chunkID;
                int offset;
                int length;
                int ret;

                if (request.data != null) {
                    System.out.println(String.format("%s|%s|%d", cmd, fileName, request.data.length));
                }

                switch (cmd.charAt(0)) {
                    case 'r':
                        chunkID = Integer.valueOf(request.params.get(0));
                        offset = Integer.valueOf(request.params.get(1));
                        length = Integer.valueOf(request.params.get(2));
                        FileChunk chunk = getChunk(fileName, chunkID);
                        if (chunk == null) {
                            response.setError(FileClient.CHUNK_NOT_AVAILABLE);
                            break;
                        }
                        char[] data = readChunk(chunk);
                        if (data != null) {
                            response.setData(Arrays.copyOfRange(data, offset, offset + length));
                        }
                        break;
                    case 'w':
                        chunkID = Integer.valueOf(request.params.get(0));
                        int actualLength = Helper.charArrayLength(request.data);
                        FileChunk chunk1 = new FileChunk(fileName, chunkID, actualLength);
                        //System.out.println(Arrays.toString(request.data));

                        int size1;
                        ArrayList<FileChunk> fileChunkList = fileInfo.fileChunks.get(fileName);
                        synchronized (fileInfo.fileChunks) {
                            if (fileChunkList == null) {
                                fileChunkList = new ArrayList<>();
                                fileInfo.fileChunks.put(fileName, fileChunkList);
                            }
                        }
                        synchronized (fileChunkList) {
                            size1 = writeChunk(chunk1, request.data);
                        }

                        addToMetaData(chunk1);
                        int size1 = write(fileName, chunkID, actualLength, request.data);
                        response.addParam(Integer.toString(size1));
                        break;
                    case 'a':
                        chunkID = Integer.valueOf(request.params.get(0));
                        ret = append(fileName, chunkID, request.data);
                        if (ret < 0) {
                            response.setError(ret);
                            break;
                        }
                        response.addParam(Integer.toString(ret));
                        break;
                    case 'd':
                        String fileNameToDelete = request.fileName;
                        int affected = deleteFile(fileNameToDelete);
                        response.addParam(Integer.toString(affected));
                        // update meta server
                        sendACKTOMeta();
                        break;
                    default:
                        System.out.println("Unknown command");
                        response.setError(FileClient.INVALID_COMMAND);
                }

                ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());
                output.writeObject(response);
                output.flush();
                output.close();

                //clientSock.close();

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Get file chunk by file name and ID
     *
     * @param fileName real file
     * @param chunkID  ID
     * @return chunk if found, otherwise null
     */
    private FileChunk getChunk(String fileName, int chunkID) {
        List<FileChunk> chunkMap = fileInfo.fileChunks.get(fileName);
        if (chunkMap == null) {
            return null;
        }
        for (FileChunk chunk : chunkMap) {
            if (chunk.chunkID == chunkID) {
                return chunk;
            }
        }
        return null;
    }

    /**
     * Read file chunk from disk
     *
     * @param chunk chunk controller block
     * @return the data read
     */
    private char[] readChunk(FileChunk chunk) {
        if (chunk == null) {
            return null;
        }

        String filePath = storageDir + "/" + chunk.getChunkName();

        try {
            FileReader reader = new FileReader(filePath);

            char[] buffer = new char[FileChunk.FIXED_SIZE];
            int size = reader.read(buffer, 0, FileChunk.FIXED_SIZE);

            if (size != FileChunk.FIXED_SIZE) {
                System.out.println("Chunk size not equals to " + FileChunk.FIXED_SIZE);
                return null;
            }

            return buffer;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Write data in buffer to path specified by file chunk block
     *
     * @param chunk  controller block
     * @param buffer buffer to written
     * @return -1 if write fails, otherwise the actual size written
     */
    private int writeChunk(FileChunk chunk, char[] buffer) {
        if (chunk == null) {
            return -1;
        }

        String filePath = storageDir + "/" + chunk.getChunkName();

        try {
            File file = new File(filePath);
            boolean bCr = file.createNewFile();

            if (!bCr) {
                System.out.println("File " + filePath);
            }

            FileWriter writer = new FileWriter(file);

            if (buffer == null) {
                // create empty file
                return 0;
            }

            writer.write(buffer, 0, buffer.length);

            // ensure each chunk is 8192 in size
            if (buffer.length < FileChunk.FIXED_SIZE) {
                char[] padding = new char[FileChunk.FIXED_SIZE - buffer.length];
                writer.append(new String(padding));
            }
            writer.close();

            return buffer.length;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private int write(String fileName, int chunkID, int actualLength, char[] data) {
        FileChunk chunk1 = new FileChunk(fileName, chunkID, actualLength);
        //System.out.println(Arrays.toString(request.data));
        int size;
        List<FileChunk> fileChunkList = fileInfo.fileChunks.get(fileName);
        if (fileChunkList == null) {
            return 0;
        }
        synchronized (fileChunkList) {
            size = writeChunk(chunk1, data);
        }
        addToMetaData(chunk1);
        // update meta server
        sendACKTOMeta();
        return size;
    }

    private int append(String fileName, int chunkID, char[] data) {
        int ret;

        FileChunk chunk2 = getChunk(fileName, chunkID);
        FileChunk oldChunk = getChunk(fileName, chunkID);
        if (chunk2 == null || oldChunk == null) {
            return FileClient.CHUNK_NOT_AVAILABLE;
        }
        List<FileChunk> fileChunkList = fileInfo.fileChunks.get(fileName);
        synchronized (fileChunkList) {
            ret = appendChunk(chunk2, data);
        }
        if (ret < 0 || ret != data.length) {
            return FileClient.FILE_LENGTH_EXCEED;
        }
        chunk2.actualLength = oldChunk.actualLength + ret;
        updateMetaData(chunk2);
        // update meta server
        sendACKTOMeta();
        return ret;
    }

    /**
     * Delete all chunks of this file
     *
     * @param fileName to delete
     * @return chunks affected
     */
    private int deleteFile(String fileName) {
        List<FileChunk> chunks = fileInfo.fileChunks.get(fileName);
        if (chunks == null) {
            return 0;
        }

        // delete from disk
        int num = 0;
        for (FileChunk chunk : chunks) {
            File chunkFile = new File(storageDir + "/" + chunk.getChunkName());
            if (chunkFile.delete()) {
                num++;
            }
        }

        // then delete from file info
        synchronized (fileInfo) {
            fileInfo.fileChunks.remove(fileName);
        }

        return num;
    }

    /**
     * Add chunk information to meta data
     *
     * @param chunk control block
     */
    private void addToMetaData(FileChunk chunk) {
        if (chunk == null) {
            return;
        }

        synchronized (fileInfo.fileChunks) {
            List<FileChunk> chunkMap = fileInfo.fileChunks.get(chunk.realFileName);
            if (chunkMap == null) {
                chunkMap = Collections.synchronizedList(new ArrayList<FileChunk>());
                fileInfo.fileChunks.put(chunk.realFileName, chunkMap);
            }
            chunkMap.add(chunk);
            Collections.sort(chunkMap);
        }
    }

    /**
     * Update the meta data, mainly update actual length
     *
     * @param chunk control block
     */
    private void updateMetaData(FileChunk chunk) {
        if (chunk == null) {
            return;
        }

        List<FileChunk> list = fileInfo.fileChunks.get(chunk.realFileName);
        if (list == null) {
            return;
        }
        for (FileChunk ck : list) {
            if (ck.chunkID == chunk.chunkID) {
                ck.actualLength = chunk.actualLength;
                return;
            }
        }
    }

    /**
     * Append data at the end of specified chunk, the size of data can not exceed
     * the remaining space of chunk. (null filled space). If exceed, buffer got truncated
     *
     * @param chunk  to be appended
     * @param buffer data
     * @return -1 if fails, otherwise the actual size of data appended
     */
    private int appendChunk(FileChunk chunk, char[] buffer) {
        if (chunk == null) {
            return -1;
        }

        // get previous data
        char[] data = readChunk(chunk);
        if (data == null) {
            System.out.println("Append must occur when the specified chunk already in disk");
            return -1;
        }
        int dataLength = Helper.charArrayLength(data);

        // data need to write exceed chunk limit
        if (FileChunk.FIXED_SIZE - dataLength < buffer.length) {
            return FileClient.FILE_LENGTH_EXCEED;
        }

        // fill the data with content in buffer
        int actualLengthAppended = Math.min(FileChunk.FIXED_SIZE - dataLength, buffer.length);
        System.arraycopy(buffer, 0, data, dataLength, actualLengthAppended);

        // overwrite old chunk
        writeChunk(chunk, data);

        return actualLengthAppended;
    }

    /**
     * Send ACK to meta server carrying with chunk information, and receive commit from server
     *
     * @return true if ACK send to meta and meta response commit. Otherwise false
     */
    private boolean sendACKTOMeta() {
        try {
            Socket toMetaSock = new Socket(metaServer.metaServerAddress, metaServer.ackPort);
            ACKEnvelop ack = ACKEnvelop.fileServerAck(this.id, this.fileInfo);
            ObjectOutputStream output = new ObjectOutputStream(toMetaSock.getOutputStream());
            output.writeObject(ack);
            output.flush();

            ObjectInputStream input = new ObjectInputStream(toMetaSock.getInputStream());
            ACKEnvelop ackFromMeta = (ACKEnvelop) input.readObject();

            input.close();

            if (ackFromMeta.type != ACKEnvelop.META_SERVER_ACK || ackFromMeta.ackNo != ack.ackNo) {
                return false;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Run this before any procedure
     */
    private void initialize() {
        fileInfo.setFileDir(this.storageDir);
        fileInfo.recoverFileInfoFromDisk();
        metaServer.resolveAddress();
        resolveAddress();
    }

    private void keepLive() {
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Launch all processes
     */
    public void launch() {
        initialize();

        prepareToSendHeartbeat();

        prepareToReceiveRequest();

        keepLive();
    }

    public static void main(String[] args) {
        FileServer fileServer = new FileServer();
        fileServer.parseXML("configs.xml");
        fileServer.launch();
    }
}
