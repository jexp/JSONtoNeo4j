package org.neo4j.jsontoneo4j;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.Arrays.asList;


/**
 * @author rafael
 */
public class Graph {

    private static final Logger LOGGER = Logger.getLogger(Graph.class.getName());
    private GraphDatabaseService graphDb;
    private Index<Node> idIndex;
    private String idKey;


    public Graph(String dbPath, boolean newDb, String idKey) {
        if (newDb) {
           clearDb(dbPath);
        }
        graphDb = new EmbeddedGraphDatabase(dbPath);
        this.idKey = idKey;
        idIndex = graphDb.index().forNodes(this.idKey);
    }

    public Graph(GraphDatabaseService graphDb, String idKey) {
        this.graphDb = graphDb;
        this.idKey = idKey;
    }

    private void clearDb(String dbPath) {
        try {
            FileUtils.deleteRecursively(new File(dbPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void shutDown() {
        if (graphDb!=null) graphDb.shutdown();
    }

    private Node createUpdateIndexNode(final Object id, final Map<String, Object> properties) {
        return new UniqueFactory.UniqueNodeFactory(idIndex) {
            protected void initialize(Node node, Map<String, Object> params) {
                setProperties(node, properties);
            }
        }.getOrCreate(idKey, id);
    }

    private Node setProperties(Node node, Map<String, Object> properties) {
        for (Entry<String, Object> entry : properties.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Collection) {
                node.setProperty(key, ((Collection)value).toArray());
            } else {
                node.setProperty(key, value);
            }
        }
        return node;
    }

    private Node createNotIndexNode(Map<String, Object> properties) {
        return setProperties(graphDb.createNode(),properties);
    }

    public void startJSONArray(JSONArray array) {
        Transaction tx = graphDb.beginTx();
        try {
            addJSONArray(array);
            tx.success();
        } catch (JSONException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            tx.failure();
        } finally {
            tx.finish();
        }
    }

    private void addJSONArray(JSONArray array) throws JSONException {
        for (int i = 0; i < array.length(); i++) {
            addJSONObject((JSONObject) array.get(i));
        }
    }

    private Map<String, Object> getProperties(JSONObject object) throws JSONException {
        Map<String, Object> properties = new HashMap<String, Object>();
        Iterator keys = object.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            final Object value = object.get(key);
            if (key == JSONObject.NULL || value == JSONObject.NULL || value instanceof JSONObject) continue;
            if (value instanceof JSONArray) {
                final List<Object> list = toList((JSONArray) value);
                if (list.isEmpty() || list.get(0) instanceof JSONObject) continue;
                properties.put(key, list);
            } else {
                properties.put(key, value);
            }
        }
        return properties;
    }
    private Node addJSONObject(JSONObject object) throws JSONException {
        Map<String, Object> properties = getProperties(object);
        Node node = object.has(idKey) ? createUpdateIndexNode(object.get(idKey), properties) : createNotIndexNode(properties);

        for (Entry<String, List<JSONObject>> entry : getRelated(object).entrySet()) {
            final DynamicRelationshipType type = DynamicRelationshipType.withName(entry.getKey());
            for (JSONObject jsonObject : entry.getValue()) {
                final Node n2 = addJSONObject(jsonObject);
                if (n2.hasProperty(idKey)) {
                    node.createRelationshipTo(n2, type);
                } else {
                    // why?
                    for (Relationship rel : n2.getRelationships(Direction.OUTGOING)) {
                        node.createRelationshipTo(rel.getEndNode(), type);
                        rel.delete();
                    }
                    n2.delete();
                }
            }
        }
        return node;
    }

    private Map<String, List<JSONObject>> getRelated(JSONObject object) throws JSONException {
        Map<String, List<JSONObject>> related = new HashMap<String, List<JSONObject>>();
        Iterator keys = object.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            if (key == JSONObject.NULL) continue;
            final Object value = object.get(key);
            if (value == JSONObject.NULL) continue;
            if (value instanceof JSONObject) related.put(key, asList((JSONObject) value));
            if (value instanceof JSONArray) {
                final List list = toList((JSONArray) value);
                if (list.isEmpty()) continue;
                if (list.get(0) instanceof JSONObject) {
                    final List<JSONObject> objectList = (List<JSONObject>) list;
                    related.put(key, objectList);
                }
            }
        }
        return related;
    }

    private List<Object> toList(JSONArray value) throws JSONException {
        final ArrayList<Object> result = new ArrayList<Object>(value.length());
        for (int i = 0; i < value.length(); i++) {
            result.add(value.get(i));
        }
        return result;
    }
}
