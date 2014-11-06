import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by evangileon on 11/5/14.
 */
public class MetaServer {

    String hostname;

    InetAddress metaServerAddress;
    int receiveHeartbeatPort;
    int clientPort;

    ServerSocket receiveHeartbeatSock;

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
            Node root = doc.getDocumentElement();
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
                System.exit(0);
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
    }

    private void resolveAllFileServerAddress() {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            pair.getValue().resolveAddress();
        }
    }

    public void launch() {
        resolveAllFileServerAddress();
        prepareToReceiveHeartbeat();
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

            while (true) {
                try {
                    InputStream tcpFlow = fileServerSock.getInputStream();
                    ObjectInputStream objectInput = new ObjectInputStream(tcpFlow);

                    FileInfo fileInfo = (FileInfo) objectInput.readObject();
                    tcpFlow.close();

                    fileInfo.print();

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

            try {
                fileServerSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("id" + id + " heartbeat exit");
        }
    }

    public static void main(String[] args) {
        MetaServer metaServer = new MetaServer();
        metaServer.parseXML("./configs.xml");
        metaServer.launch();
    }
}
