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
import java.util.concurrent.ConcurrentHashMap;

public class MetaServer {

    String hostname;

    InetAddress metaServerAddress;
    // port to listen to heartbeat connections
    int receiveHeartbeatPort;
    // port to listen to client requests
    int clientPort;
    // port to receive ACKs
    int ackPort;
    // replica fetch port
    int replicaPort;

    // socket to listen to heartbeat connections
    ServerSocket receiveHeartbeatSock;
    // socket to listen to client requests
    ServerSocket receiveRequestSock;
    // socket to listen to ACKs
    ServerSocket receiveAckSock;
    // socket to listen to replica fetch request
    ServerSocket receiveReplicaFetch;

    // String -> list of Chunks -> location
    // map all chunks of a file to file servers, primary replica
    final Map<String, List<Integer>> fileChunkMap = new ConcurrentHashMap<>();
    // replica 2
    final Map<String, List<Integer>> fileChunkMapReplica2 = new ConcurrentHashMap<>();
    // replica 3
    final Map<String, List<Integer>> fileChunkMapReplica3 = new ConcurrentHashMap<>();

    // records the availability  of all chunks of a file
    //final Map<String, List<Boolean>> fileChunkAvailableMap = new ConcurrentHashMap<>();

    // file server id -> file info
    // map the file server id to file information on this file server
    final Map<Integer, FileInfo> fileServerInfoMap = new ConcurrentHashMap<>();

    // pending file chunks not send to file servers
    // file name -> hash map to chunk id -> file server id expected to store
    //final Map<String, Map<Integer, Integer>> pendingFileChunks = new ConcurrentHashMap<>();

    // fail times of heartbeat correspondent to id
    //final Map<Integer, Integer> fileServerFailTimes = Collections.synchronizedMap(new HashMap<Integer, Integer>());

    // store necessary information about file servers
    final TreeMap<Integer, FileServer> allFileServerList = new TreeMap<>();
    // store the availability of file servers
    final TreeMap<Integer, Boolean> allFileServerAvail = new TreeMap<>();

    // latest time the file server with id send heartbeat to meta server
    final Map<Integer, Long> fileServerTouch = new ConcurrentHashMap<>();
    // times of haven't receive file server heartbeat
    final Map<Integer, Integer> fileServerHeartbeatFailTimes = new ConcurrentHashMap<>();

    // load balance control
    LoadBalancer loadBalancer;

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

            String nodeName = oneConfig.getNodeName();
            String text = oneConfig.getTextContent();
            if (nodeName.equals("hostname")) {
                this.hostname = text;
            }
            if (nodeName.equals("receiveHeartbeatPort")) {
                this.receiveHeartbeatPort = Integer.parseInt(text);
            }
            if (nodeName.equals("clientPort")) {
                this.clientPort = Integer.parseInt(text);
            }
            if (nodeName.equals("ackPort")) {
                this.ackPort = Integer.parseInt(text);
            }
            if (nodeName.equals("replicaPort")) {
                this.replicaPort = Integer.parseInt(text);
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
            System.exit(-1);
        }

        Thread heartbeatHandleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {

                    try {
                        Socket fileServerSock = receiveHeartbeatSock.accept();
                        int id = identifyConnection(fileServerSock);
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
            System.exit(-1);
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
                        e.printStackTrace();
                    }
                }
                //System.out.println("Exit request handle loop");
            }
        });

        requestHandleThread.setDaemon(true);
        requestHandleThread.start();
    }

    /**
     * Create a new thread to listen to all ACKs.
     */
    private void prepareToReceiveACK() {
        try {
            receiveAckSock = new ServerSocket(this.ackPort);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Thread ackThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {

                    try {
                        Socket ackSock = receiveAckSock.accept();
                        ObjectInputStream input = new ObjectInputStream(ackSock.getInputStream());
                        ACKEnvelop ack = (ACKEnvelop) input.readObject();

                        if (ack.type == ACKEnvelop.FILE_SERVER_ACK) {
                            int remoteID = identifyConnection(ackSock);

                            // update file chunk information in meta server
                            synchronizeWithMap(remoteID, ack.fileInfo);

                        } else if (ack.type == ACKEnvelop.CLIENT_ACK) {

                        }

                        ACKEnvelop ackResponse = ACKEnvelop.metaServerAck(ack.ackNo);

                        ObjectOutputStream output = new ObjectOutputStream(ackSock.getOutputStream());
                        output.writeObject(ackResponse);
                        output.flush();
                        output.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }
        });

        ackThread.setDaemon(true);
        ackThread.start();
    }

    /**
     * Create a new thread to handle the fetch replicas request from file server
     */
    private void prepareToReceiveReplicaRequest() {
        try {
            receiveReplicaFetch = new ServerSocket(this.replicaPort);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Thread replicaThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket replicaFetchSock = receiveReplicaFetch.accept();
                        ObjectInputStream input = new ObjectInputStream(replicaFetchSock.getInputStream());
                        RequestEnvelop request = (RequestEnvelop) input.readObject();

                        ResponseEnvelop response = new ResponseEnvelop(request);

                        if (request.cmd.equals("fetchReplicas")) {
                            List<Integer> replicas = getReplicas(request.fileName, request.chunkID);
                            response.chunksLocation = new LinkedList<>(replicas);
                        }

                        ObjectOutputStream output = new ObjectOutputStream(replicaFetchSock.getOutputStream());
                        output.writeObject(response);
                        output.flush();

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }
        });

        replicaThread.setDaemon(true);
        replicaThread.start();
    }

    /**
     * Search the remote hostname of new accepted socket on configuration files, to identify
     * the heartbeat connection.
     *
     * @param fileServerSock the newly accepted socket by heartbeat server socket
     * @return the id of the file server server socket
     */
    private int identifyConnection(Socket fileServerSock) {
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

        FileInfo fileInfo = fileServerInfoMap.get(id);

        if (fileInfo == null) {
            return;
        }

        synchronized (fileServerInfoMap) {
            fileServerInfoMap.remove(id);
        }

        migrateReplicas(id, fileInfo);
    }

    /**
     * Migrate all chunks in the file server to proper file servers
     *
     * @param id       failed server
     * @param fileInfo info in the failed server
     */
    private void migrateReplicas(int id, FileInfo fileInfo) {
        for (Map.Entry<String, List<FileChunk>> pair : fileInfo.fileChunks.entrySet()) {
            String fileName = pair.getKey();
            List<FileChunk> chunkList = pair.getValue();

            for (FileChunk chunk : chunkList) {
                List<Integer> replicas = getReplicas(fileName, chunk.chunkID);
                int primaryReplica = getPrimaryReplica(fileName, chunk.chunkID);
                if (primaryReplica < 0 || replicas == null) {
                    continue;
                }

                replicas.add(0, primaryReplica);
                int newReplica = loadBalancer.getExclusiveReplica(replicas);

                if (isPrimaryReplica(id, fileName, chunk.chunkID)) {
                    // primary replica
                    // primary need to switch over to a another replica

                    // get replica 2 become primary
                    synchronized (fileChunkMap) {
                        fileChunkMap.get(fileName).set(chunk.chunkID, replicas.get(1));
                    }
                    // move 3 to 2
                    synchronized (fileChunkMapReplica2) {
                        fileChunkMapReplica2.get(fileName).set(chunk.chunkID, replicas.get(2));
                    }
                    // add new replica to 3
                    synchronized (fileChunkMapReplica3) {
                        fileChunkMapReplica3.get(fileName).set(chunk.chunkID, newReplica);
                    }

                    int from = fileChunkMapReplica2.get(fileName).get(chunk.chunkID);
                    int ret = requestFileServerToMigrateReplica(from, newReplica, fileName, chunk.chunkID);
                    if (ret < 0) {
                        System.out.println("Migrate fail: from " + from + " to " + newReplica);
                    }
                } else {
                    if (fileChunkMapReplica2.get(fileName).get(chunk.chunkID) == id) {
                        // replica 2 fail
                        // move 3 to 2
                        synchronized (fileChunkMapReplica2) {
                            fileChunkMapReplica2.get(fileName).set(chunk.chunkID, newReplica);
                        }
                    } else {
                        // replica 3 fail
                        synchronized (fileChunkMapReplica3) {
                            fileChunkMapReplica3.get(fileName).set(chunk.chunkID, newReplica);
                        }
                    }
                    int primary = getPrimaryReplica(fileName, chunk.chunkID);
                    if (primary > 0) {
                        int ret = requestFileServerToMigrateReplica(primary, newReplica, fileName, chunk.chunkID);
                        if (ret < 0) {
                            System.out.println("Migrate fail: from " + primary + " to " + newReplica);
                        }
                    }
                }
            }
        }
    }

    /**
     * Get the ID of file server that the chunk store
     *
     * @param fileName file name
     * @param chunkID  ID
     * @return id of the file server that the chunk store
     */
    private int getPrimaryReplica(String fileName, int chunkID) {
        List<Integer> list;
        synchronized (fileChunkMap) {
            list = fileChunkMap.get(fileName);
        }
        if (list == null) {
            return -1;
        }

        if (list.size() <= chunkID) {
            return -1;
        }
        return list.get(chunkID);
    }

    /**
     * Request file server to copy its chunk to another server
     *
     * @param from     file server
     * @param to       file server
     * @param fileName file name
     * @param chunkID  ID
     * @return negative if fail
     */
    private int requestFileServerToMigrateReplica(int from, int to, String fileName, int chunkID) {

        if (!allFileServerAvail.containsKey(from) || !allFileServerAvail.get(from)) {
            return FileClient.FILE_SERVER_NOT_AVAILABLE;
        }

        FileServer fileServer = allFileServerList.get(from);
        if (fileServer == null) {
            return FileClient.FILE_SERVER_NOT_AVAILABLE;
        }

        try {
            Socket fileSock = new Socket(fileServer.fileServerAddress, fileServer.commandPort);
            RequestEnvelop request = new RequestEnvelop("migrateReplica", fileName);
            request.chunkID = chunkID;
            request.addParam(Integer.toString(to));

            ObjectOutputStream output = new ObjectOutputStream(fileSock.getOutputStream());
            output.writeObject(request);
            output.flush();

            ObjectInputStream input = new ObjectInputStream(fileSock.getInputStream());
            ResponseEnvelop response = (ResponseEnvelop) input.readObject();

            return response.error;

        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return 0;
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

            printFileChunkMap();
            //printAvailabilityMap();

            long currentTime = System.currentTimeMillis();

            synchronized (fileServerTouch) {
                for (Map.Entry<Integer, Long> pair : fileServerTouch.entrySet()) {
                    int id = pair.getKey();
                    long lastTouch = pair.getValue();

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
        }
    }

    /**
     * Because meta server store the information about file servers,
     * upon received heartbeat message, meta server need to update
     * the information according to file chunk information carried
     * by heartbeat
     *
     * @param id       file server identified by identifyConnection
     * @param fileInfo heartbeat carrying file chunk information
     */
    private void synchronizeWithMap(int id, FileInfo fileInfo) {
        if (fileInfo == null) {
            return;
        }

        synchronized (fileServerInfoMap) {
            fileServerInfoMap.put(id, fileInfo);
        }
        synchronized (allFileServerAvail) {
            allFileServerAvail.put(id, true);
        }
    }

    /**
     * Get replicas arrangement
     *
     * @param fileName file name
     * @param chunkID  ID
     * @return list of replica locations
     */
    private List<Integer> getReplicas(String fileName, int chunkID) {
        ArrayList<Integer> replicas = new ArrayList<>();

        List<Integer> replicaList2 = fileChunkMapReplica2.get(fileName);
        if (replicaList2 == null) {
            return null;
        }
        if (replicaList2.size() <= chunkID) {
            return null;
        }
        replicas.add(replicaList2.get(chunkID));

        List<Integer> replicaList3 = fileChunkMapReplica3.get(fileName);
        if (replicaList3 == null) {
            return null;
        }
        if (replicaList3.size() <= chunkID) {
            return null;
        }
        replicas.add(replicaList3.get(chunkID));

        assert replicas.size() == 2;
        return replicas;
    }

    /**
     * Add replicas for specified file, specified chunk
     *
     * @param fileName file name
     * @param chunkID  ID
     * @param replicas list
     */
    private void addToReplicaList(String fileName, int chunkID, List<Integer> replicas) {
        if (replicas == null || replicas.size() != 2) {
            return;
        }

        List<Integer> replicaList2;
        synchronized (fileChunkMapReplica2) {
            replicaList2 = fileChunkMapReplica2.get(fileName);
            if (replicaList2 == null) {
                replicaList2 = Collections.synchronizedList(new ArrayList<Integer>());
                fileChunkMapReplica2.put(fileName, replicaList2);
            }
        }

        synchronized (replicaList2) {
            Helper.expandToIndexInteger(replicaList2, chunkID);
            replicaList2.set(chunkID, replicas.get(0));
        }

        List<Integer> replicaList3;
        synchronized (fileChunkMapReplica3) {
            replicaList3 = fileChunkMapReplica3.get(fileName);
            if (replicaList3 == null) {
                replicaList3 = Collections.synchronizedList(new ArrayList<Integer>());
                fileChunkMapReplica3.put(fileName, replicaList3);
            }
        }

        synchronized (replicaList3) {
            Helper.expandToIndexInteger(replicaList3, chunkID);
            replicaList3.set(chunkID, replicas.get(1));
        }
    }

    /**
     * If the chunk information sent from file server primary
     *
     * @param id       ID that send the chunk info
     * @param fileName file name
     * @param chunkID  chunk ID
     * @return whether primary
     */
    private boolean isPrimaryReplica(int id, String fileName, int chunkID) {
        List<Integer> chunkList = fileChunkMap.get(fileName);
        if (chunkList == null) {
            return false;
        }
        return chunkList.size() > chunkID && chunkList.get(chunkID) == id;
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

    @SuppressWarnings("unused")
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

                if (command.length() > 1) {

                    if (command.equals("fetchReplicas")) {
                        String fileReplica = request.fileName;
                        int chunkIDReplica = request.chunkID;

                        List<Integer> replicas = getReplicas(fileReplica, chunkIDReplica);

                        if (replicas != null) {
                            response.chunksLocation = new LinkedList<>(replicas);
                        } else {
                            response.setError(-1);
                        }
                    }

                } else {

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
                }

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
    private int read(String fileName, int offset, int length, List<Integer> chunkList, List<Integer> chunkLocationList) {
        chunkList.clear();
        chunkLocationList.clear();

        if (!fileChunkMap.containsKey(fileName)) {
            return FileClient.FILE_NOT_EXIST;
        }

        // number of full chunks before the offset.
        int offsetBelongsToWhichChunk = offset / FileChunk.FIXED_SIZE;
        int lastIndex = offset + length - 1;
        int lastIndexBelongsToWhichChunk = lastIndex / FileChunk.FIXED_SIZE;

        // need to scan this list of chunks
        LinkedList<Integer> chunksNeedToScan = new LinkedList<>();
        for (int i = offsetBelongsToWhichChunk; i <= lastIndexBelongsToWhichChunk; i++) {
            chunksNeedToScan.add(i);
        }

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null) {
            return FileClient.FILE_NOT_EXIST;
        }
        for (Integer chunkID : chunksNeedToScan) {
            if (chunkID >= list.size()) {
                return FileClient.FILE_LENGTH_EXCEED;
            }
            int location = list.get(chunkID);
            if (!checkAvailability(location, fileName, chunkID)) {
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

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null || list.size() == 0) {
            return FileClient.FILE_NOT_EXIST;
        }

        // availability
        if (!checkAvailability(list.get(list.size() - 1), fileName, list.size() - 1)) {
            return FileClient.CHUNK_NOT_AVAILABLE;
        }

        int whereLastChunk = list.get(list.size() - 1);
        // the chunk you demand is in file server whereLastChunk
        List<FileChunk> chunks = fileServerInfoMap.get(whereLastChunk).fileChunks.get(fileName);
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

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null) {
            return FileClient.FILE_NOT_EXIST;
        }

        // availability
        if (!checkAvailability(list.get(list.size() - 1), fileName, list.size() - 1)) {
            return FileClient.CHUNK_NOT_AVAILABLE;
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

        List<Integer> list = fileChunkMap.get(fileName);
        if (list == null) {
            return FileClient.FILE_NOT_EXIST;
        }

        // availability
        if (!checkAvailability(list.get(list.size() - 1), fileName, list.size() - 1)) {
            return FileClient.CHUNK_NOT_AVAILABLE;
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
    private int write(String fileName, int length, List<Integer> chunkList, List<Integer> chunkLocationList) {

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
            int location = random.nextInt(availFileServers.size());
            chunkList.add(i);
            chunkLocationList.add(idArray[location]);
        }

        // arrange replicas
        Iterator<Integer> chunkItor = chunkList.iterator();
        Iterator<Integer> locationItor = chunkLocationList.iterator();
        while (chunkItor.hasNext()) {
            int chunk = chunkItor.next();
            int loc = locationItor.next();

            List<Integer> replicas = loadBalancer.getReplicas(loc);
            if (replicas != null) {
                addToReplicaList(fileName, chunk, replicas);
            }
        }

        // update meta data
        List<Integer> list = Collections.synchronizedList(new ArrayList<Integer>());
        locationItor = chunkLocationList.iterator();
        while (locationItor.hasNext()) {
            list.add(locationItor.next());
        }
        synchronized (fileChunkMap) {
            fileChunkMap.put(fileName, list);
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
    private int append(String fileName, int length, List<Integer> chunkList, List<Integer> chunkLocationList) {
        chunkList.clear();
        chunkLocationList.clear();

        if (!fileChunkMap.containsKey(fileName)) {
            return FileClient.FILE_NOT_EXIST;
        }

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

        // only distribute to available file servers
        ArrayList<Integer> availFileServers = new ArrayList<>();
        for (Integer key : allFileServerList.keySet()) {
            if (allFileServerAvail.containsKey(key) && allFileServerAvail.get(key)) {
                availFileServers.add(key);
            }
        }

        Integer[] idArray = new Integer[availFileServers.size()];
        idArray = availFileServers.toArray(idArray);

        // randomly distribute new chunks
        for (int i = lastChunk + 1; i <= newLastChunk; i++) {
            int location = random.nextInt(availFileServers.size());
            chunkList.add(i);
            chunkLocationList.add(idArray[location]);
        }

        // update meta data
        Iterator<Integer> chunkItor = chunkList.iterator();
        Iterator<Integer> locationItor = chunkLocationList.iterator();

        if (lastRemain != 0) {
            // append chunks is already there
            chunkItor.next();
            locationItor.next();
        }

        List<Integer> list = fileChunkMap.get(fileName);
        synchronized (list) {
            while (locationItor.hasNext()) {
                list.add(locationItor.next());
            }
        }

        chunkItor = chunkList.iterator();
        locationItor = chunkLocationList.iterator();
        if (lastRemain != 0) {
            // first append chunks is already there
            chunkItor.next();
            locationItor.next();
        }

        // arrange replicas
        chunkItor = chunkList.iterator();
        locationItor = chunkLocationList.iterator();
        while (chunkItor.hasNext()) {
            int chunk = chunkItor.next();
            int loc = locationItor.next();

            List<Integer> replicas = loadBalancer.getReplicas(loc);
            if (replicas != null) {
                addToReplicaList(fileName, chunk, replicas);
            }
        }

        return FileChunk.FIXED_SIZE - lastRemain;
    }

    /**
     * Delete files. Delete chunks from file servers with file name
     *
     * @param fileName to delete
     * @return deleted successfully
     */
    private boolean delete(String fileName) {
        List<Integer> chunkLocations = fileChunkMap.get(fileName);

        if (chunkLocations == null) {
            return false;
        }

        // check all availability
        int num = 0;
        for (Integer location : chunkLocations) {
            if (!checkAvailability(location, fileName, num++)) {
                return false;
            }
        }

        int affected = 0;
        // broadcast delete request to file servers
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            FileServer fileServer = pair.getValue();
            RequestEnvelop request = new RequestEnvelop("d", fileName);

            try {
                Socket fileSock = new Socket(fileServer.fileServerAddress, fileServer.requestFilePort);
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
        synchronized (fileChunkMapReplica2) {
            fileChunkMapReplica2.remove(fileName);
        }
        synchronized (fileChunkMapReplica3) {
            fileChunkMapReplica3.remove(fileName);
        }

        System.out.println(String.format("%s: %d chunks deleted", fileName, affected));
        return true;
    }

    /**
     * Get the load list of each alive file servers
     *
     * @return map
     */
    public Map<Integer, Integer> getChunkNumberMap() {
        HashMap<Integer, Integer> map = new HashMap<>();
        for (Integer id : fileServerInfoMap.keySet()) {
            map.put(id, 0);
        }

        for (List<Integer> chunks : fileChunkMap.values()) {
            for (Integer location : chunks) {
                if (map.containsKey(location)) {
                    map.put(location, map.get(location) + 1);
                }
            }
        }
        for (List<Integer> chunks : fileChunkMapReplica2.values()) {
            for (Integer location : chunks) {
                if (map.containsKey(location)) {
                    map.put(location, map.get(location) + 1);
                }
            }
        }
        for (List<Integer> chunks : fileChunkMapReplica3.values()) {
            for (Integer location : chunks) {
                if (map.containsKey(location)) {
                    map.put(location, map.get(location) + 1);
                }
            }
        }
        return map;
    }

    /**
     * Check the Availability of chunk at file server
     *
     * @param id       file server
     * @param fileName file name
     * @param chunkID  ID
     * @return true if available, false otherwise
     */
    private boolean checkAvailability(int id, String fileName, int chunkID) {
        FileInfo fileInfo = fileServerInfoMap.get(id);
        if (fileInfo == null) {
            return false;
        }

        List<FileChunk> chunkList = fileInfo.fileChunks.get(fileName);
        if (chunkList == null) {
            return false;
        }

        for (FileChunk chunk : chunkList) {
            if (chunk.chunkID == chunkID) {
                return true;
            }
        }
        return false;
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

        loadBalancer = new LoadBalancer(this);
    }

    /**
     * Launch all work process
     */
    public void launch() {
        initialize();

        prepareToReceiveReplicaRequest();

        prepareToReceiveHeartbeat();

        prepareToReceiveACK();

        prepareToReceiveClientRequest();

        keepCheckingLivenessOfHeartbeat();
    }

    public static void main(String[] args) {
        MetaServer metaServer = new MetaServer();
        metaServer.parseXML("./configs.xml");
        metaServer.launch();

    }
}
