import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;

/**
 * Created by Jun Yu on 11/5/14.
 */
public class FileServer {

    String hostname;
    int id;
    boolean isRealMachine;

    MetaServer metaServer;
    InetAddress thisFileServerAddress;

    // the port this server opens to other file servers
    int fileServerExchangePort;

    HashMap<Integer, FileServer> allFileServerList;

    /**
     * Parse XML to acquire hostname, ports of process
     * @param filename
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
                    if(oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                        continue;
                    }

                    if (oneConfig.getNodeName().equals("id")) {
                        id = Integer.parseInt(oneConfig.getTextContent());
                    }
                    if (oneConfig.getNodeName().equals("hostname")) {
                        hostname = oneConfig.getTextContent();
                    }
                    if(oneConfig.getNodeName().equals("recvPort")) {
                        recvPort = Integer.parseInt(oneConfig.getTextContent());
                    }
                    if(oneConfig.getNodeName().equals("acksPort")) {
                        acksPort = Integer.parseInt(oneConfig.getTextContent());
                    }
                }
                this.allFileServerList.put(id, new FileServer());
            }

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }
}
