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
import java.io.*;
import java.net.*;
import java.util.*;

public class MetaServer {

    String hostname;

    InetAddress metaServerAddress;
    // port to listen to heartbeat connections
    int receiveHeartbeatPort;
    // port to listen to client requests
    int clientPort;
    // port to receive ACKs
    int ackPort;

    // socket to listen to heartbeat connections
    ServerSocket receiveHeartbeatSock;
    // socket to listen to client requests
    ServerSocket receiveRequestSock;

    // String -> list of Chunks -> location
    // map all chunks of a file to file servers
    final Map<String, List<Integer>> fileChunkMap = Collections.synchronizedMap(new HashMap<String, List<Integer>>());
    // records the availability  of all chunks of a file
    final Map<String, List<Boolean>> fileChunkAvailableMap = Collections.synchronizedMap(new HashMap<String, List<Boolean>>());

    // file server id -> file info
    // map the file server id to file information on this file server
    final Map<Integer, FileInfo> fileServerInfoMap = Collections.synchronizedMap(new HashMap<Integer, FileInfo>());

    // pending file chunks not send to file servers
    // file name -> hash map to chunk id -> file server id expected to store
    final Map<String, Map<Integer, Integer>> pendingFileChunks = Collections.synchronizedMap(new HashMap<String, Map<Integer, Integer>>());

    // fail times of heartbeat correspondent to id
    //final Map<Integer, Integer> fileServerFailTimes = Collections.synchronizedMap(new HashMap<Integer, Integer>());

    // store necessary information about file servers
    final TreeMap<Integer, FileServer> allFileServerList = new TreeMap<>();
    // store the availability of file servers
    final TreeMap<Integer, Boolean> allFileServerAvail = new TreeMap<>();

    // latest time the file server with id send heartbeat to meta server
    final Map<Integer, Long> fileServerTouch = Collections.synchronizedMap(new HashMap<Integer, Long>());
    // times of haven't receive file server heartbeat
    final Map<Integer, Integer> fileServerHeartbeatFailTimes = Collections.synchronizedMap(new HashMap<Integer, Integer>());

    // termination flag
    boolean terminated = false;

    // timeout of heartbeat on established connection
    int timeoutMillis = 5000;

    public MetaServer() {
    }


    public MetaServer(Node serverNode) {
        parseXMLToConfigMetaServer(serverNode);
    }

    /**
     * Resolve the address of meta server
     */
    public void resolveAddress() {
        if (hostname == null) {
            return;
        }

        try {
            metaServerAddress = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse XML to acquire hostname, ports of process
     *
     * @param filename xml
     */
    private void parseXML(String filename) {

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
            parseXMLToConfigMetaServer(metaServerNode);
            // config for file server virtual machine
            parseXMLToConfigFileServers(doc);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            if (e instanceof SAXException) {
                System.out.println("XML is not valid");
                System.exit(0);
            }
            e.printStackTrace();
        }
    }

    /**
     * Retrieve information of all file servers
     *
     * @param doc XML object
     */
    private void parseXMLToConfigFileServers(Document doc) {
        Node fileServers = doc.getElementsByTagName("fileServers").item(0);
        NodeList fileServerList = fileServers.getChildNodes();

        for (int i = 0; i < fileServerList.getLength(); i++) {
            Node oneServer = fileServerList.item(i);
            if (oneServer.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            NodeList serverConfig = oneServer.getChildNodes();
            int id = 0;


            for (int j = 0; j < serverConfig.getLength(); j++) {
                Node oneConfig = serverConfig.item(j);
                if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }

                if (oneConfig.getNodeName().equals("id")) {
                    id = Integer.parseInt(oneConfig.getTextContent());
                }

            }
            this.allFileServerList.put(id, new FileServer(id, oneServer));
        }
    }

    /**
     * Retrieve all information about meta server
     *
     * @param serverNode root element node of meta server in XML config file
     */
    private void parseXMLToConfigMetaServer(Node serverNode) {

        NodeList serverConfig = serverNode.getChildNodes();

        for (int j = 0; j < serverConfig.getLength(); j++) {
            Node oneConfig = serverConfig.item(j);
            if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            if (oneConfig.getNodeName().equals("hostname")) {
                this.hostname = oneConfig.getTextContent();
            }
            if (oneConfig.getNodeName().equals("receiveHeartbeatPort")) {
                this.receiveHeartbeatPort = Integer.parseInt(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("clientPort")) {
                this.clientPort = Integer.parseInt(oneConfig.getTextContent());
            }
        }
    }

    /**
     * Wait for heartbeat connection, create a new thread.
     */
    private void prepareToReceiveHeartbeat() {
        System.out.println("Meta server receive heartbeat port: " + receiveHeartbeatPort);
        System.out.println("Number of file servers: " + allFileServerList.size());
        try {
            receiveHeartbeatSock = new ServerSocket(receiveHeartbeatPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread heartbeatHandleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket fileServerSock = receiveHeartbeatSock.accept();
                        int id = identifyHeartbeatConnection(fileServerSock);
                        if (id < 0) {
                            System.out.println("Unknown address; " + fileServerSock.getInetAddress());
                            continue;
                        }

                        System.out.println("Heartbeat connection from: " + fileServerSock.getInetAddress().toString());
                        // one file server, one heartbeat thread
                        HeartbeatEntity oneHeartbeatEntity = new HeartbeatEntity(id, fileServerSock);
                        Thread thread = new Thread(oneHeartbeatEntity);
                        thread.setDaemon(true);
                        thread.start();

                    } catch (IOException e) {
                        if (e instanceof SocketException) {
                            System.out.println(e.toString());
                            break;
                        }

                        e.printStackTrace();
                    }
                }
            }
        });

        heartbeatHandleThread.setDaemon(true);
        heartbeatHandleThread.start();
    }

    /**
     * Create a new thread to listen to all client requests.
     */
    private void prepareToReceiveClientRequest() {
        try {
            receiveRequestSock = new ServerSocket(clientPort);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        Thread requestHandleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket clientSock = receiveRequestSock.accept();
                        System.out.println("Receive request from: " + clientSock.getInetAddress().toString());

                        ResponseFileRequestEntity responseFileRequestEntity = new ResponseFileRequestEntity(clientSock);
                        Thread threadResponse = new Thread(responseFileRequestEntity);
                        threadResponse.setDaemon(true);
                        threadResponse.start();
                    } catch (IOException e) {

                        if (e instanceof SocketException) {
                            break;
                        }

                        e.printStackTrace();
                    }
                }
                System.out.println("Exit request handle loop");
            }
        });

        requestHandleThread.setDaemon(true);
        requestHandleThread.start();
    }

    /**
     * Search the remote hostname of new accepted socket on configuration files, to identify
     * the heartbeat connection.
     *
     * @param fileServerSock the newly accepted socket by heartbeat server socket
     * @return the id of the file server server socket
     */
    private int identifyHeartbeatConnection(Socket fileServerSock) {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            if (pair.getValue().fileServerAddress.equals(fileServerSock.getInetAddress())) {
                return pair.getKey();
            }
        }

        return -1;
    }

    /**
     * Notify meta server that file server with id fails
     *
     * @param id of file server
     */
    private void fileServerFail(int id) {

        System.out.println("File server fail three times: " + id);

        // file server in unavailable
        synchronized (allFileServerAvail) {
            allFileServerAvail.put(id, false);
        }

        // update availability
        synchronized (fileChunkAvailableMap) {
            FileInfo fileInfo = fileServerInfoMap.get(id);

            if (fileInfo != null) {
                for (Map.Entry<String, ArrayList<FileChunk>> pair : fileInfo) {
                    String fileName = pair.getKey();
                    ArrayList<FileChunk> fileChunks = pair.getValue();

                    List<Boolean> availableMap;
                    availableMap = fileChunkAvailableMap.get(fileName);
                    if (availableMap == null) {
                        availableMap = new ArrayList<>();
                        fileChunkAvailableMap.put(fileName, availableMap);
                    }

                    for (FileChunk fileChunk : fileChunks) {
                        Helper.expandToIndexBoolean(availableMap, fileChunk.chunkID);
                        availableMap.set(fileChunk.chunkID, false);
                    }
                }
            }
        }
    }

    /**
     * Resolve all IP address of file servers
     */
    private void resolveAllFileServerAddress() {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            pair.getValue().resolveAddress();
        }
    }

    /**
     * File server heartbeat delivered to meta
     *
     * @param id of file server
     */
    private void fileServerHeartbeatTouch(int id) {
        // set latest time that file server touch to current time
        Long currentTime = System.currentTimeMillis();
        synchronized (fileServerTouch) {
            fileServerTouch.put(id, currentTime);
        }
        synchronized (allFileServerAvail) {
            allFileServerAvail.put(id, true);
        }
    }

    /**
     * File server heartbeat fail one time
     *
     * @param id of file server
     */
    private void fileServerHeartbeatFailOneTime(int id) {
        Integer times = fileServerHeartbeatFailTimes.get(id);
        if (times == null) {
            System.out.println("Logical error");
            return;
        }

        int newTimes = times + 1;
        System.out.println("ID = " + id + " fail total time: " + newTimes);
        if (newTimes >= 3) { // heartbeat fail 3 times means file server down
            System.out.println("ID = " + id + " is down");
            fileServerFail(id);
            newTimes = 0;
        }
        synchronized (fileServerHeartbeatFailTimes) {
            fileServerHeartbeatFailTimes.put(id, newTimes);
        }
    }

    /**
     * Compare the value in fileServerTouch with current time, if the different exceeds 5 seconds,
     * the file server fail for one time.
     * This procedure runs forever
     */
    private void keepCheckingLivenessOfHeartbeat() {
        while (true) {
            try {
                // sleep 5 second first, then run every 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                continue;
            }

            //printFileChunkMap();
            //printAvailabilityMap();

            long currentTime = System.currentTimeMillis();

            synchronized (fileServerTouch) {
                for (Map.Entry<Integer, Long> pair : fileServerTouch.entrySet()) {
                    int id = pair.getKey();
                    long lastTouch = pair.getValue();

                    //System.out.println(String.format("id = %d, %s = %d, %s = %d", id, "CurrenTime", currentTime, "LastTouch", lastTouch));

                    long diff = currentTime - lastTouch;
                    if (diff > 5000) {
                        System.out.println("File server fail one time: " + id);
                        fileServerHeartbeatFailOneTime(id);
                    } else {
                        // touched within 5 seconds
                        synchronized (fileServerHeartbeatFailTimes) {
                            fileServerHeartbeatFailTimes.put(id, 0);
                        }
                    }
                }
            }

            if (terminated) {
                System.out.println("Exit timeout checking loop");
                break;
            }
        }
    }

    /**
     * Because meta server store the information about file servers,
     * upon received heartbeat message, meta server need to update
     * the information according to file chunk information carried
     * by heartbeat
     *
     * @param id       file server identified by identifyHeartbeatConnection
     * @param fileInfo heartbeat carrying file chunk information
     */
    private void synchronizeWithMap(int id, FileInfo fileInfo) {

        // for each record of file chunks on file server
        for (Map.Entry<String, ArrayList<FileChunk>> pair : fileInfo) {
            String fileName = pair.getKey();
            ArrayList<FileChunk> fileChunks = pair.getValue();

            List<Integer> chunksOnThisServer;
            // must be mutually exclusive
            synchronized (fileChunkMap) {
                chunksOnThisServer = fileChunkMap.get(fileName);
                if (chunksOnThisServer == null) {
                    chunksOnThisServer = Collections.synchronizedList(new ArrayList<Integer>());
                    fileChunkMap.put(fileName, (chunksOnThisServer));
                }
            }

            // synchronize
            synchronized (chunksOnThisServer) {
                for (FileChunk fileChunk : fileChunks) {
                    Helper.expandToIndexInteger(chunksOnThisServer, fileChunk.chunkID);
                    chunksOnThisServer.set(fileChunk.chunkID, id);
                }
            }

            // update availability
            List<Boolean> availableMap;
            synchronized (fileChunkAvailableMap) {
                availableMap = fileChunkAvailableMap.get(fileName);
                if (availableMap == null) {
                    availableMap = Collections.synchronizedList(new ArrayList<Boolean>());
                    fileChunkAvailableMap.put(fileName, availableMap);
                }
            }

            synchronized (availableMap) {
                for (FileChunk fileChunk : fileChunks) {
                    Helper.expandToIndexBoolean(availableMap, fileChunk.chunkID);
                    availableMap.set(fileChunk.chunkID, true);
                }
            }
        }

        synchronized (fileServerInfoMap) {
            fileServerInfoMap.put(id, fileInfo);
        }

    }

    /**
     * Add to pending list, if the entry for a filename not exist, just create it
     *
     * @param fileName     this file has pending chunks
     * @param chunkID      pending chunk
     * @param fileServerID pending chunk intended to store
     */
    private void addToPendingList(String fileName, int chunkID, int fileServerID) {
        Map<Integer, Integer> pending = pendingFileChunks.get(fileName);
        if (pending == null) {
            pending = Collections.synchronizedMap(new HashMap<Integer, Integer>());
            synchronized (pendingFileChunks) {
                pendingFileChunks.put(fileName, pending);
            }
        }
        synchronized (pending) {
            pending.put(chunkID, fileServerID);
        }
    }

    /**
     * Remove from pending list. The removing happens if and only if three parameters exist,
     * and match to pending list. Otherwise do nothing.
     *
     * @param fileName     this file has pending chunks
     * @param chunkID      pending chunk
     * @param fileServerID pending chunk intended to store
     */
    private void removeFromPendingList(String fileName, int chunkID, int fileServerID) {
        Map<Integer, Integer> pending = pendingFileChunks.get(fileName);
        if (pending == null) {
            return;
        }
        if (!pending.containsKey(chunkID)) {
            return;
        }
        if (pending.get(chunkID) != fileServerID) {
            return;
        }
        synchronized (pending) {
            pending.remove(chunkID);
        }
        // remove file entry
        if (pending.size() == 0) {
            synchronized (pendingFileChunks) {
                pendingFileChunks.remove(fileName);
            }
        }
    }

    /**
     * Release the pending file chunks, that means all chunks that client supposed
     * to upload are already uploaded to file servers. Because file server send the
     * information about these chunks already in its disk.
     *
     * @param id       the id from which the file info sent
     * @param fileInfo transited from file servers
     */
    private void releasePendingChunks(int id, FileInfo fileInfo) {

        for (Map.Entry<String, ArrayList<FileChunk>> fileChunksInFileServer : fileInfo) {
            String fileName = fileChunksInFileServer.getKey();
            if (!pendingFileChunks.containsKey(fileName)) {
                continue;
            }

            ArrayList<FileChunk> chunks = fileChunksInFileServer.getValue();
            for (FileChunk chunk : chunks) {
                int chunkID = chunk.chunkID;
                // check whether ID equal to keep consistent
                removeFromPendingList(fileName, chunkID, id);
            }
        }
    }


    /**
     * Thread to handle single heartbeat connection
     */
    class HeartbeatEntity implements Runnable {
        Socket fileServerSock;

        int id;

        public HeartbeatEntity(int id, Socket fileServerSock) {
            this.id = id;
            this.fileServerSock = fileServerSock;
            fileServerSock.getInetAddress();

            fileServerHeartbeatFailTimes.put(id, 0);

            try {
                fileServerSock.setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            System.out.println("Enter meta server heartbeat receive loop");
            while (true) {
                try {
                    InputStream tcpFlow = fileServerSock.getInputStream();
                    ObjectInputStream objectInput = new ObjectInputStream(tcpFlow);

                    FileInfo fileInfo = (FileInfo) objectInput.readObject();

                    System.out.println("Heartbeat received from " + id);

                    //fileInfo.print();
                    //System.out.println("fileInfo printed");

                    // update file chunk information in meta server
                    synchronizeWithMap(this.id, fileInfo);

                    // check and release pending chunks, these chunks are already in file servers
                    releasePendingChunks(this.id, fileInfo);

                    // file server touched meta server
                    fileServerHeartbeatTouch(this.id);

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();

                    break;
                }
            }

            try {
                fileServerSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("id " + id + " heartbeat exit");
        }
    }

    private void printFileChunkMap() {
        System.out.println("File Chunk Map:");
        for (Map.Entry<String, List<Integer>> pair : fileChunkMap.entrySet()) {
            System.out.printf("%s: ", pair.getKey());
            List<Integer> list = pair.getValue();
            for (Integer id : list) {
                System.out.print(id + ", ");
            }
            System.out.println();
        }
    }

    private void printAvailabilityMap() {
        System.out.println("File Chunk Availability Map:");
        for (Map.Entry<String, List<Boolean>> pair : fileChunkAvailableMap.entrySet()) {
            System.out.printf("%s: ", pair.getKey());
            for (Boolean avail : pair.getValue()) {
                System.out.print(avail + ", ");
            }
            System.out.println();
        }
    }

    /**
     * Thread to handle single client request
     */
    class ResponseFileRequestEntity implements Runnable {

        Socket clientSock;

        public ResponseFileRequestEntity(Socket clientSock) {
            this.clientSock = clientSock;
        }

        @Override
        public void run() {

            try {

                ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());
                RequestEnvelop request = (RequestEnvelop) input.readObject();

                String command = request.cmd;
                String fileName = request.fileName;

                System.out.println(command + "|" + fileName);
                ResponseEnvelop response = new ResponseEnvelop(request);
                System.out.println("Response UUID: " + response.uuid.toString());

                if (command.length() != 1) {
                    return;
                }

                char cmd = command.charAt(0);
                int error = 0;
                Integer offset;
                Integer length;
                LinkedList<Integer> chunkList = new LinkedList<>();
                LinkedList<Integer> chunkLocationList = new LinkedList<>();

                switch (cmd) {
                    case 'r':
                        // TODO read file
                        if (request.params.size() != 2) {
                            error = FileClient.INVALID_COMMAND;
                            break;
                        }
                        offset = Integer.valueOf(request.params.get(0));
                        length = Integer.valueOf(request.params.get(1));

                        error = read(fileName, offset, length, chunkList, chunkLocationList);

                        break;
                    case 'a':
                        // TODO append file
                        if (request.params.size() != 1) {
                            error = FileClient.INVALID_COMMAND;
                            break;
                        }
                        length = Integer.valueOf(request.params.get(0));

                        int offseta = append(fileName, length, chunkList, chunkLocationList);
                        if (offseta >= 0) {
                            response.addParam(Integer.toString(offseta % FileChunk.FIXED_SIZE)); // offset
                        }

                        break;
                    case 'w':
                        // TODO write file
                        if (request.params.size() != 1) {
                            error = FileClient.INVALID_COMMAND;
                            break;
                        }

                        length = Integer.valueOf(request.params.get(0));

                        error = write(fileName, length, chunkList, chunkLocationList);

                        break;
                    case 'd':
                        // TODO delete file
                        boolean deleted = delete(fileName);
                        if (!deleted) {
                            error = FileClient.FILE_NOT_EXIST;
                        }

                        break;
                    default:
                        error = FileClient.INVALID_COMMAND;
                        System.out.println("Unknown command: " + cmd);
                }

                if (error < 0) {
                    response.setError(error);
                } else {
                    response.setChunksToScan(chunkList);
                    response.setChunksLocation(chunkLocationList);
                }

                System.out.println("Number of chunks affected: " + chunkList.size());
                ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());
                output.writeObject(response);
                output.flush();
                output.close();

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            System.out.println("Exit response to: " + clientSock.getInetAddress().toString());
        }

    }

    /**
     * Read file from meta data
     *
     * @param fileName          file to read
     * @param offset            first start index inclusive
     * @param length            to read
     * @param chunkList         to store chunks to scan
     * @param chunkLocationList to store location of chunks correspondent to chunkList
     * @return error code
     */
    public int read(String fileName, int offset, int length, List<Integer> chunkList, List<Integer> chunkLocationList) {
        chunkList.clear();
        chunkLocationList.clear();
        // number of full chunks before the offset.
        int offsetBelongsToWhichChunk = offset / FileChunk.FIXED_SIZE;
        int lastIndex = offset + length - 1;
        int lastIndexBelongsToWhichChunk = lastIndex / FileChunk.FIXED_SIZE;

        // need to scan this list of chunks
        LinkedList<Integer> chunksNeedToScan = new LinkedList<>();
        for (int i = offsetBelongsToWhichChunk; i <= lastIndexBelongsToWhichChunk; i++) {
            chunksNeedToScan.add(i);
        }

        // check pending list
        Map<Integer, Integer> pending = pendingFileChunks.get(fileName);
        if (pending != null) {
            for (Integer chunkID : chunksNeedToScan) {
                if (pending.containsKey(chunkID)) {
                    return FileClient.CHUNK_IN_PENDING;
                }
            }
        }

        // check availability
        List<Boolean> avails = fileChunkAvailableMap.get(fileName);
        if (avails == null) {
            return FileClient.FILE_NOT_EXIST;
        }
        // check whether demanded chunk exists, in other word, demanded length not exceed real length
        if (lastIndexBelongsToWhichChunk >= avails.size()) {
            return FileClient.FILE_LENGTH_EXCEED;
        }
        for (Integer chunkID : chunksNeedToScan) {
            if (!avails.get(chunkID)) {
                return FileClient.CHUNK_NOT_AVAILABLE;
            }
        }

        // get chunk list and location
        chunkList.addAll(chunksNeedToScan);
        List<Integer> locations = fileChunkMap.get(fileName);
        for (Integer chunkID : chunkList) {
            chunkLocationList.add(locations.get(chunkID));
        }

        return FileClient.SUCCESS;
    }

    /**
     * Check whether last chunk of file is full
     *
     * @param fileName file
     * @return 0 if is full, otherwise the remaining space, -1 if not available, -2 if in pending
     */
    private int checkLastChunkOfFile(String fileName) {
        if (pendingFileChunks.containsKey(fileName)) {
            return FileClient.CHUNK_IN_PENDING;
        }

        List<Boolean> avails = fileChunkAvailableMap.get(fileName);
        boolean availableOfLast = avails.get(avails.size() - 1);
        if (!availableOfLast) {
            return FileClient.CHUNK_NOT_AVAILABLE;
        }

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null) {
            return FileClient.FILE_NOT_EXIST;
        }

        int whereLastChunk = list.get(list.size() - 1);
        // the chunk you demand is in file server whereLastChunk
        ArrayList<FileChunk> chunks = fileServerInfoMap.get(whereLastChunk).fileChunks.get(fileName);
        FileChunk lastChunk = chunks.get(chunks.size() - 1);
        int actualLength = lastChunk.actualLength;
        return FileChunk.FIXED_SIZE - actualLength;
    }

    /**
     * Get the ID of last chunk of the file
     *
     * @param fileName file
     * @return chunk ID of last chunk, or error code
     */
    private int getLastChunkOfFile(String fileName) {
        if (pendingFileChunks.containsKey(fileName)) {
            return FileClient.CHUNK_IN_PENDING;
        }

        List<Boolean> avails = fileChunkAvailableMap.get(fileName);
        boolean availableOfLast = avails.get(avails.size() - 1);
        if (!availableOfLast) {
            return FileClient.CHUNK_NOT_AVAILABLE;
        }

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null) {
            return FileClient.FILE_NOT_EXIST;
        }
        return list.size() - 1;
    }

    /**
     * Get the location of last chunk of the file
     *
     * @param fileName file
     * @return file server ID of last chunk, or error code
     */
    private int getLocationOfLastChunkOfFile(String fileName) {
        if (pendingFileChunks.containsKey(fileName)) {
            return FileClient.CHUNK_IN_PENDING;
        }

        List<Boolean> avails = fileChunkAvailableMap.get(fileName);
        boolean availableOfLast = avails.get(avails.size() - 1);
        if (!availableOfLast) {
            return FileClient.CHUNK_NOT_AVAILABLE;
        }

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null) {
            return FileClient.FILE_NOT_EXIST;
        }
        return list.get(list.size() - 1);
    }

    /**
     * Write to meta data, it will add new file chunks to pending list,
     * Then heartbeat message will synchronize meta data map, and remove from pending list in the future
     *
     * @param fileName          to create
     * @param length            of new file
     * @param chunkList         list of chunk to be created
     * @param chunkLocationList list of chunk location at file server
     * @return 0 if succeed, otherwise error code
     */
    public int write(String fileName, int length, List<Integer> chunkList, List<Integer> chunkLocationList) {

        chunkList.clear();
        chunkLocationList.clear();

        // overwrite
        if (fileChunkMap.containsKey(fileName)) {
            delete(fileName);
        }

        int lastChunk = (length - 1) / FileChunk.FIXED_SIZE;
        Random random = new Random();

        // only distribute to available file servers
        ArrayList<Integer> availFileServers = new ArrayList<>();
        for (Integer key : allFileServerList.keySet()) {
            if (allFileServerAvail.containsKey(key) && allFileServerAvail.get(key)) {
                availFileServers.add(key);
            }
        }

        Integer[] idArray = new Integer[availFileServers.size()];
        idArray = availFileServers.toArray(idArray);

        // randomly distribute chunks
        for (int i = 0; i <= lastChunk; i++) {
            int location = random.nextInt(allFileServerList.size());
            chunkList.add(i);
            chunkLocationList.add(idArray[location]);
        }

        // add to pending list
        Iterator<Integer> chunkItor = chunkList.iterator();
        Iterator<Integer> locationItor = chunkLocationList.iterator();
        while (chunkItor.hasNext()) {
            int chunk = chunkItor.next();
            int loc = locationItor.next();
            addToPendingList(fileName, chunk, loc);
        }


        return FileClient.SUCCESS;
    }

    final Random random = new Random();

    /**
     * Append data to file, may update last non-full chunk
     *
     * @param fileName          file
     * @param length            to append
     * @param chunkList         list of chunks affected
     * @param chunkLocationList list of location of chunks
     * @return offset that begin to append if success, or error code
     */
    public int append(String fileName, int length, List<Integer> chunkList, List<Integer> chunkLocationList) {
        chunkList.clear();
        chunkLocationList.clear();

        int lastRemain = checkLastChunkOfFile(fileName);
        if (lastRemain < 0) {
            return lastRemain; // error code
        }

        // last chunk, which is the only chunk may be non-full
        int lastChunk = getLastChunkOfFile(fileName);
        if (lastRemain > 0) {
            // need update the non-full chunk
            chunkList.add(lastChunk);
            chunkLocationList.add(getLocationOfLastChunkOfFile(fileName));
        }

        int dataRemain = length - lastRemain;
        if (dataRemain <= 0) {
            // all data arranged
            return FileChunk.FIXED_SIZE - lastRemain;
        }

        // then append entirely new chunks
        int newLastChunk = lastChunk + 1 + (dataRemain - 1) / FileChunk.FIXED_SIZE;

        Integer[] idArray = new Integer[allFileServerList.size()];
        idArray = allFileServerList.keySet().toArray(idArray);

        // randomly distribute new chunks
        for (int i = lastChunk + 1; i <= newLastChunk; i++) {
            int location = random.nextInt(allFileServerList.size());
            chunkList.add(i);
            chunkLocationList.add(idArray[location]);
        }

        // add to pending list
        Iterator<Integer> chunkItor = chunkList.iterator();
        Iterator<Integer> locationItor = chunkLocationList.iterator();
        while (chunkItor.hasNext()) {
            int chunk = chunkItor.next();
            int loc = locationItor.next();
            addToPendingList(fileName, chunk, loc);
        }

        return FileChunk.FIXED_SIZE - lastRemain;
    }

    /**
     * Delete files. Delete chunks from file servers with file name
     *
     * @param fileName to delete
     * @return deleted successfully
     */
    public boolean delete(String fileName) {
        List<Integer> chunkLocations = fileChunkMap.get(fileName);
        List<Boolean> chunkAvails = fileChunkAvailableMap.get(fileName);

        if (chunkAvails == null || chunkLocations == null) {
            return false;
        }

        // check pending list
        if (pendingFileChunks.containsKey(fileName)) {
            return false;
        }

        // check all availability
        for (Boolean chunkAvail : chunkAvails) {
            if (!chunkAvail) {
                return false;
            }
        }

        int affected = 0;
        // broadcast delete request to file servers
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            FileServer fileServer = pair.getValue();
            RequestEnvelop request = new RequestEnvelop("d", fileName);

            try {
                Socket fileSock = new Socket(fileServer.hostname, fileServer.requestFilePort);
                ObjectOutputStream output = new ObjectOutputStream(fileSock.getOutputStream());
                output.writeObject(request);
                output.flush();

                ObjectInputStream input = new ObjectInputStream(fileSock.getInputStream());
                ResponseEnvelop response = (ResponseEnvelop) input.readObject();

                if (response.params != null && response.params.size() > 0) {
                    affected += Integer.valueOf(response.params.get(0));
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }

        // delete from metadata
        synchronized (fileChunkMap) {
            fileChunkMap.remove(fileName);
        }
        synchronized (fileChunkAvailableMap) {
            fileChunkAvailableMap.remove(fileName);
        }

        System.out.println(String.format("%s: %d chunks deleted", fileName, affected));
        return true;
    }

    /**
     * Thread to handle ACKs sent from file servers or clients
     */
    class HandleAckEntity implements Runnable {

        @Override
        public void run() {

        }
    }

    /**
     * Run before any procedure
     */
    private void initialize() {
        resolveAllFileServerAddress();

        for (Integer id : allFileServerList.keySet()) {
            // -1 means never touched
            fileServerTouch.put(id, (long) -1);
        }

        for (Integer id : allFileServerList.keySet()) {
            // -1 means never touched
            fileServerHeartbeatFailTimes.put(id, 0);
        }
    }

    /**
     * Launch all work process
     */
    public void launch() {
        initialize();

        prepareToReceiveClientRequest();

        prepareToReceiveHeartbeat();

        keepCheckingLivenessOfHeartbeat();
    }

    public static void main(String[] args) {
        MetaServer metaServer = new MetaServer();
        metaServer.parseXML("./configs.xml");
        metaServer.launch();

    }
}
