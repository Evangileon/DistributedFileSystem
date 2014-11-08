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
    int receiveHeartbeatPort;
    int clientPort;

    ServerSocket receiveHeartbeatSock;
    ServerSocket receiveRequestSock;

    // String -> list of Chunks -> location
    final HashMap<String, List<Integer>> fileChunkMap = new HashMap<>();
    final HashMap<String, List<Boolean>> fileChunkAvailableMap = new HashMap<>();

    // file server id -> file info
    final HashMap<Integer, FileInfo> fileServerInfoMap = new HashMap<>();


    int timeoutMillis;

    HashMap<Integer, FileServer> allFileServerList;

    public MetaServer() {
    }


    public MetaServer(Node serverNode) {
        parseXMLToConfigMetaServer(serverNode);
    }

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
     * Wait for heartbeat connection
     */
    public void prepareToReceiveHeartbeat() {
        System.out.println("Meta server receive heartbeat port: " + receiveHeartbeatPort);
        try {
            receiveHeartbeatSock = new ServerSocket(receiveHeartbeatPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                Socket fileServerSock = receiveHeartbeatSock.accept();
                int id = identifyHeartbeatConnection(fileServerSock);
                if (id < 0) {
                    System.out.println("Unknown address; " + fileServerSock.getInetAddress());
                    continue;
                }

                System.out.println("Heartbeat connection from: " + fileServerSock.getInetAddress().toString());
                HeartbeatEntity oneHeartbeatEntity = new HeartbeatEntity(id, fileServerSock);
                Thread thread = new Thread(oneHeartbeatEntity);
                thread.setDaemon(true);
                thread.start();
            } catch (IOException e) {
                e.printStackTrace();
                //System.exit(0);
            }
        }
    }

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

    private void resolveAllFileServerAddress() {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            pair.getValue().resolveAddress();
        }
    }

    public void launch() {
        resolveAllFileServerAddress();

        ResponseFileRequestEntity responseFileRequestEntity = new ResponseFileRequestEntity();
        Thread threadResponse = new Thread(responseFileRequestEntity);
        threadResponse.setDaemon(true);
        threadResponse.start();

        prepareToReceiveHeartbeat();
    }

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

    public static <T> void expandToIndex(List<T> list, int index) {
        int size = list.size();

        for (int i = 0; i < (index + 1 - size); i++) {
            list.add((T) (new Object()));
        }
    }

    class HeartbeatEntity implements Runnable {
        Socket fileServerSock;
        int failTimes;
        int id;

        public HeartbeatEntity(int id, Socket fileServerSock) {
            failTimes = 0;

            this.id = id;
            this.fileServerSock = fileServerSock;
            fileServerSock.getInetAddress();
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
                    System.out.println("fileInfo printed");

                    synchronizeWithMap(this.id, fileInfo);

                } catch (IOException e) {
                    e.printStackTrace();

                    if (e instanceof SocketTimeoutException) {
                        failTimes++;

                        if (failTimes >= 3) {
                            fileServerFail(id);
                            // exit this heartbeat thread
                            break;
                        }
                    } else {
                        failTimes++;

                        if (failTimes >= 3) {
                            fileServerFail(id);
                            // exit this heartbeat thread
                            break;
                        }
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
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

    class ResponseFileRequestEntity implements Runnable {

        @Override
        public void run() {
            try {
                receiveRequestSock = new ServerSocket(clientPort);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            while (true) {
                try {
                    Socket clientSock = receiveRequestSock.accept();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));

                    String line = reader.readLine();
                    if (line == null | line.equals("")) {
                        continue;
                    }

                    String[] params = line.split("\\|");

                    if (params.length < 3) {
                        continue;
                    }

                    String command = params[0];
                    String fileName = params[1];

                    if (command.length() != 1) {
                        continue;
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

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        MetaServer metaServer = new MetaServer();
        metaServer.parseXML("./configs.xml");
        metaServer.launch();

    }
}
