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
    int receiveHeartbeatPort;
    ServerSocket receiveHeartbeatSock;
    InetAddress thisAddress;

    int timeoutMillis;

    HashMap<Integer, FileServer> allFileServerList;

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

            // System.out.println(doc.getDocumentElement().getNodeName());
            Node fileServers = doc.getElementsByTagName("FileServers").item(0);
            // System.out.println(fileServers.item(0).getNodeName());
            NodeList fileServerList = fileServers.getChildNodes();

            for (int i = 0; i < fileServerList.getLength(); i++) {
                Node oneMachine = fileServerList.item(i);
                if (oneMachine.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }

                NodeList machineConfig = oneMachine.getChildNodes();
                int id = 0;
                String hostname = null;
                int recvPort = 0;
                int acksPort = 0;

                for (int j = 0; j < machineConfig.getLength(); j++) {
                    Node oneConfig = machineConfig.item(j);
                    if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                        continue;
                    }

                    if (oneConfig.getNodeName().equals("id")) {
                        id = Integer.parseInt(oneConfig.getTextContent());
                    }
                    if (oneConfig.getNodeName().equals("hostname")) {
                        hostname = oneConfig.getTextContent();
                    }
                    if (oneConfig.getNodeName().equals("recvPort")) {
                        recvPort = Integer.parseInt(oneConfig.getTextContent());
                    }
                    if (oneConfig.getNodeName().equals("acksPort")) {
                        acksPort = Integer.parseInt(oneConfig.getTextContent());
                    }
                }
                this.allFileServerList.put(id, new FileServer());
            }

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Wait for heartbeat connection
     */
    public void prepareToReceiveHeartbeat() {

        try {
            while (true) {
                Socket fileServerSock = receiveHeartbeatSock.accept();
                int id = identifyHeartbeatConnection(fileServerSock);
                if (id < 0) {
                    System.out.println("Unknown address; " + fileServerSock.getInetAddress());
                    continue;
                }

                HeartbeatEntity oneHeartbeatEntity = new HeartbeatEntity(id, fileServerSock);
                Thread thread = new Thread(oneHeartbeatEntity);
                thread.setDaemon(true);
                thread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int identifyHeartbeatConnection(Socket fileServerSock) {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            if (pair.getValue().thisFileServerAddress.equals(fileServerSock.getInetAddress())) {
                return pair.getKey();
            }
        }

        return -1;
    }

    /**
     * Notify meta server that file server with id fails
     * @param id of file server
     */
    public void fileServerFail(int id) {

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
            try {
//                BufferedReader input = new BufferedReader(new InputStreamReader(fileServerSock.getInputStream()));
//                String line;
//                while ((line = input.readLine()) != null) {
//                    failTimes = 0;
//
//                    // filename tab chunk_number tab chunk number new line
//                    String[] params = line.split("\t");
//                    if (params.length <= 1) {
//                        System.out.println(params);
//                    }
//
//                    String fileName = params[0];
//                    String[] chunkNumbers = new String[params.length - 1];
//                    System.arraycopy(params, 1, chunkNumbers, 0, params.length - 1);
//
//
//                }
                InputStream tcpFlow = fileServerSock.getInputStream();
                ObjectInputStream objectInput = new ObjectInputStream(tcpFlow);

                FileInfo fileInfo = (FileInfo) objectInput.readObject();

            } catch (IOException e) {
                e.printStackTrace();
                if (e instanceof  SocketTimeoutException) {
                    failTimes++;

                    if (failTimes >= 3) {
                        fileServerFail(id);
                        // exit this heartbeat thread
                        return;
                    }
                } else {
                    failTimes++;

                    if (failTimes >= 3) {
                        fileServerFail(id);
                        // exit this heartbeat thread
                        return;
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
