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

    // socket to listen to heartbeat connections
    ServerSocket receiveHeartbeatSock;
    // socket to listen to client requests
    ServerSocket receiveRequestSock;

    // String -> list of Chunks -> location
    // map all chunks of a file to file servers
    final HashMap<String, List<Integer>> fileChunkMap = new HashMap<>();
    // records the availability  of all chunks of a file
    final HashMap<String, List<Boolean>> fileChunkAvailableMap = new HashMap<>();

    // file server id -> file info
    // map the file server id to file information on this file server
    final HashMap<Integer, FileInfo> fileServerInfoMap = new HashMap<>();

    // pending file chunks not send to file servers
    // file name -> hash map to chunk id -> file server id expected to store
    final HashMap<String, HashMap<Integer, Integer>> pendingFileChunks = new HashMap<>();

    // fail times of heartbeat correspondent to id
    final HashMap<Integer, Integer> fileServerFailTimes = new HashMap<>();

    // timeout of heartbeat on established connection
    int timeoutMillis = 5000;

    // store necessary information about file servers
    HashMap<Integer, FileServer> allFileServerList;

    // latest time the file server with id send heartbeat to meta server
    final HashMap<Integer, Long> fileServerTouch = new HashMap<>();
    // times of haven't receive file server heartbeat
    final HashMap<Integer, Integer> fileServerHeartbeatFailTimes = new HashMap<>();

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
            parseXMLToConfigMetaServer(metaServerNode);
            // config for file server virtual machine
            parseXMLToConfigFileServers(doc);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieve information of all file servers
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
    public void prepareToReceiveHeartbeat() {
        System.out.println("Meta server receive heartbeat port: " + receiveHeartbeatPort);
        try {
            receiveHeartbeatSock = new ServerSocket(receiveHeartbeatPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread headbeatHandleThread = new Thread(new Runnable() {
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

        headbeatHandleThread.setDaemon(true);
        headbeatHandleThread.start();
    }

    /**
     * Create a new thread to listen to all client requests.
     */
    public void prepareToReceiveClientRequest() {
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
     * @param fileServerSock the newly accepted socket by heartbeat server socket
     * @return the id of the file server server socket
     */
    public int identifyHeartbeatConnection(Socket fileServerSock) {
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
    public void fileServerFail(int id) {

        System.out.println("File server fail: " + id);

        // update availability
        synchronized (fileChunkAvailableMap) {
            FileInfo fileInfo = fileServerInfoMap.get(id);

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
                    expandToIndex(availableMap, fileChunk.chunkID);
                    availableMap.set(fileChunk.chunkID, false);
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

    private void fileServerHeartbeatTouch(int id) {
        // set latest time that file server touch to current time
        Long currentTime = System.currentTimeMillis();
        fileServerTouch.put(id, currentTime);
        // set fail times to zero
        fileServerFailTimes.put(id, 0);
    }

    /**
     * File server heartbeat fail one time
     * @param id of file server
     */
    private void fileServerHeartbeatFailOneTime(int id) {
        Integer times = fileServerHeartbeatFailTimes.get(id);
        if (times == null) {
            System.out.println("Logical error");
            return;
        }

        times++;
        if (times >= 3) { // heartbeat fail 3 times means file server down
            fileServerFail(id);
            times = 0;
        }
        fileServerFailTimes.put(id, times);
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

            long currentTime = System.currentTimeMillis();

            synchronized (fileServerTouch) {
                for (Map.Entry<Integer, Long> pair : fileServerTouch.entrySet()) {
                    int id = pair.getKey();
                    long lastTouch = pair.getValue();
                    if (lastTouch < 0) {
                        // never touch

                    }

                    long diff = currentTime - lastTouch;
                    if (diff > 5000) {
                        fileServerHeartbeatFailOneTime(id);
                    }
                }
            }
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

    /**
     * Because meta server store the information about file servers,
     * upon received heartbeat message, meta server need to update
     * the information according to file chunk information carried
     * by heartbeat
     * @param id file server identified by identifyHeartbeatConnection
     * @param fileInfo heartbeat carrying file chunk information
     */
    public void synchronizeWithMap(int id, FileInfo fileInfo) {

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
                    expandToIndex(chunksOnThisServer, fileChunk.chunkID);
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
                    expandToIndex(availableMap, fileChunk.chunkID);
                    availableMap.set(fileChunk.chunkID, true);
                }
            }
        }

        synchronized (fileServerInfoMap) {
            fileServerInfoMap.put(id, fileInfo);
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
    public void releasePendingChunks(int id, FileInfo fileInfo) {
        for (Map.Entry<String, ArrayList<FileChunk>> fileChunksInFileServer : fileInfo) {
            String fileName = fileChunksInFileServer.getKey();
            if (!pendingFileChunks.containsKey(fileName)) {
                continue;
            }

            HashMap<Integer, Integer> pendingChunks = pendingFileChunks.get(fileName);
            ArrayList<FileChunk> chunks = fileChunksInFileServer.getValue();
            for (FileChunk chunk : chunks) {
                int chunkID = chunk.chunkID;

                // check whether ID equal to keep consistent
                if (pendingChunks.containsKey(chunkID) && pendingChunks.get(chunkID) == id) {
                    pendingChunks.remove(chunkID);
                }
            }
        }
    }

    /**
     * Because ArrayList list has a stupid property: it can not access the object which
     * exceed current size. Some elements need to be append to index
     * @param list list to be expanded
     * @param index the index you want to access
     * @param <T> Object
     */
    @SuppressWarnings("unchecked")
    public static <T> void expandToIndex(List<T> list, int index) {
        int size = list.size();

        for (int i = 0; i < (index + 1 - size); i++) {
            list.add((T) new Object());
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

            fileServerFailTimes.put(id, 0);


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

                    fileInfo.print();
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
            System.out.println("Exit meta server heartbeat receive loop");

            try {
                fileServerSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("id " + id + " heartbeat exit");
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
                Socket clientSock = receiveRequestSock.accept();

                ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());
                RequestEnvelop request = (RequestEnvelop) input.readObject();

                String command = request.cmd;
                String fileName = request.fileName;

                System.out.println(command + "|" + fileName);
                ResponseEnvelop response = new ResponseEnvelop(request);
                System.out.println("Response UUID: " + response.uuid.toString());

                ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());
                output.writeObject(response);

                if (command.length() != 1) {
                    return;
                }

                char cmd = command.charAt(0);

                switch (cmd) {
                    case 'r':
                        // TODO read file
                        break;
                    case 'a':
                        // TODO append file
                        break;
                    case 'w':
                        // TODO write file
                        break;
                    case 'd':
                        // TODO delete file
                        break;
                    default:
                        System.out.println("Unknown command: " + cmd);
                }

                clientSock.close();

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            System.out.println("Exit response to: " + clientSock.getInetAddress().toString());
        }
    }

    public static void main(String[] args) {
        MetaServer metaServer = new MetaServer();
        metaServer.parseXML("./configs.xml");
        metaServer.launch();

    }
}
