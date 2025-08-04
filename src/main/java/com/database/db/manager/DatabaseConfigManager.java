package com.database.db.manager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class DatabaseConfigManager {
    private static final Logger logger = Logger.getLogger(DatabaseConfigManager.class.getName());
    private final String path;
    private String databaseName;
    private final List<TableXML> tables = new ArrayList<>();

    public DatabaseConfigManager(String path, String name) {
        this.path = path;
        this.databaseName = name;
    }

    /** Add a table with its name, config string, and cache capacity. */
    public void addTable(String tableName, String schemaConfig, int cacheCapacity) {
        tables.add(new TableXML(tableName, schemaConfig, cacheCapacity));
    }

    public void removeTable(String tableName)  throws ParserConfigurationException, TransformerException, IOException {
        // load current state
        load();  
        // filter out the one to remove
        tables.removeIf(t -> t.tableName.equals(tableName));
        // write XML back out
        writeDatabaseXml(Paths.get(path, databaseName + ".db"), databaseName, tables);
    }

    /** Ensures file exists, then writes out XML. */
    public void create() {
        Path file = Paths.get(this.path, this.databaseName + ".db");
        try {
            if (Files.notExists(file)) {
                Files.createFile(file);
                logger.info("Created database config file: " + file);
            }
            writeDatabaseXml(file, databaseName, tables);
        } catch (IOException | ParserConfigurationException | TransformerException e) {
            logger.log(Level.SEVERE,
                    "Could not create/write config file for database '" + this.databaseName + "'.",e);
        }
    }

    /** Reads databaseName + tables back into this object. */
    public void load() {
        Path file = Paths.get(this.path, this.databaseName + ".db");
        if (Files.notExists(file)) {
            logger.warning("Config file does not exist: " + file);
            return;
        }
        try {
            readDatabaseXml(file);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                       "Failed to read config file for database '" + this.databaseName + "'.",e);
        }
    }

    // --- static helpers ------------------------------------------------

    private static void writeDatabaseXml(Path file,
                                         String databaseName,
                                         List<TableXML> tables)
            throws ParserConfigurationException, TransformerException, IOException
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();

        Document doc = db.newDocument();
        Element root = doc.createElement("database");
        doc.appendChild(root);

        // <databaseName>MyDB</databaseName>
        if (databaseName != null) {
            Element nameEl = doc.createElement("databaseName");
            nameEl.setTextContent(databaseName);
            root.appendChild(nameEl);
        }

        // <table> … </table>*
        for (TableXML t : tables) {
            Element tableEl = doc.createElement("table");

            Element tn = doc.createElement("tableName");
            tn.setTextContent(t.tableName);
            tableEl.appendChild(tn);

            Element tc = doc.createElement("schemaConfig");
            tc.setTextContent(t.schemaConfig);
            tableEl.appendChild(tc);

            Element cc = doc.createElement("cacheCapacity");
            cc.setTextContent(Integer.toString(t.cacheCapacity));
            tableEl.appendChild(cc);

            root.appendChild(tableEl);
        }

        // write out with indentation
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer tr = tf.newTransformer();
        tr.setOutputProperty(OutputKeys.INDENT, "yes");
        tr.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

        try (var os = Files.newOutputStream(file)) {
            tr.transform(new DOMSource(doc), new StreamResult(os));
        }
    }

    private void readDatabaseXml(Path file)
            throws Exception
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(Files.newInputStream(file));

        Element root = doc.getDocumentElement();

        // <databaseName>
        NodeList dnList = root.getElementsByTagName("databaseName");
        if (dnList.getLength() > 0) {
            this.databaseName = dnList.item(0).getTextContent();
        }

        // <table>
        this.tables.clear();
        NodeList tbls = root.getElementsByTagName("table");
        for (int i = 0; i < tbls.getLength(); i++) {
            Element tableEl = (Element) tbls.item(i);

            String tn = tableEl.getElementsByTagName("tableName")
                               .item(0).getTextContent();
            String tc = tableEl.getElementsByTagName("schemaConfig")
                               .item(0).getTextContent();
            String ccText = tableEl.getElementsByTagName("cacheCapacity")
                                   .item(0).getTextContent();
            int cc = Integer.parseInt(ccText);

            this.tables.add(new TableXML(tn, tc, cc));
        }
    }

    // --- Table holder class ---------------------------------------------
    public static class TableXML {
        public final String tableName;
        public final String schemaConfig;
        public final int    cacheCapacity;

        public TableXML(String tableName, String schemaConfig, int cacheCapacity) {
            this.tableName     = tableName;
            this.schemaConfig   = schemaConfig;
            this.cacheCapacity = cacheCapacity;
        }
    }

    // --- getters/setters ------------------------------------------------
    public String getDatabaseName() {
        return databaseName;
    }
    public void setDatabaseName(String dbName) {
        this.databaseName = dbName;
    }
    public List<TableXML> getTables() {
        return List.copyOf(tables);
    }
}