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
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;


public class FileServer {
    int id;
    String hostname;

    InetAddress fileServerAddress;
    int receiveMetaFilePort; // receive from meta server
    int requestFilePort; // request from client
    int fileServerExchangePort; // the port this server opens to other file servers

    MetaServer metaServer;

    FileInfo fileInfo;
    String storageDir;

    Socket heartbeatSock;

    HashMap<Integer, FileServer> allFileServerList;

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

    public static Node getFileServerNodeWithHostname(Document doc, XPath xPath, String hostname) {
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

            if (oneConfig.getNodeName().equals("id")) {
                this.id = Integer.valueOf(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("hostname")) {
                this.hostname = oneConfig.getTextContent();
            }
            if (oneConfig.getNodeName().equals("receiveMetaFilePort")) {
                this.receiveMetaFilePort = Integer.parseInt(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("requestFilePort")) {
                this.requestFilePort = Integer.parseInt(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("storageDir")) {
                this.storageDir = oneConfig.getTextContent();
            }
        }
    }

    public void heartbeatToMetaServer() {
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
                                System.out.println("fileInfo flushed");
                                //output.close();
                            } catch (IOException e) {
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
                e.printStackTrace();
                System.out.println("heartbeatToMetaServer loop IOException");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public void launch() {
        fileInfo = new FileInfo(storageDir);
        fileInfo.recoverFileInfoFromDisk();
        metaServer.resolveAddress();
        resolveAddress();
        heartbeatToMetaServer();
    }

    public static void main(String[] args) {
        FileServer fileServer = new FileServer();
        fileServer.parseXML("configs.xml");
        fileServer.launch();
    }
}
