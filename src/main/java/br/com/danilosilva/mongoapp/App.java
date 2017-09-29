package br.com.danilosilva.mongoapp;

import com.mongodb.*;

import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class App {

    public static Logger logger = Logger.getLogger("App.class");

    private static MongoClient mongoCli;

    private static final String MONGO_HOST = "localhost";
    private static final Integer MONGO_PORT = 27017;
    private static final String MONGO_DB_NAME = "local";
    private static final String MONGO_COLLECTION_NAME = "user";

    public static MongoClient getMongoClient(){
        if (mongoCli == null){
            // get a client instance on unsecure mongo instalation. Only for test purpose.
            mongoCli = new MongoClient( MONGO_HOST , MONGO_PORT );
        }
        return mongoCli;
    }



    public  static void main(String[] args){
        //DB db = getMongoClient().getDB("local"); //get db representation for local named db;

        List<String> dbs = getMongoClient().getDatabaseNames();
        logger.info("Listing dbs on mongo instance");
        for(String d : dbs){
            System.out.println(d);
        }

        BasicDBObject person1 = new BasicDBObject();
        person1.put("name", "Danilo");
        person1.put("age", 35);
        person1.put("cpf", "00012345622");
        person1.put("createdDate", new Date());

        BasicDBObject person2 = new BasicDBObject();
        person2.put("name", "J.C.");
        person2.put("age", 33);
        person2.put("cpf", "00020003300");
        person2.put("createdDate", new Date());

        BasicDBObject person3 = new BasicDBObject();
        person3.put("name", "Geronimo");
        person3.put("age", 66);
        person3.put("cpf", "12345678901");
        person3.put("createdDate", new Date());

        save(MONGO_DB_NAME, MONGO_COLLECTION_NAME, person1);
        save(MONGO_DB_NAME, MONGO_COLLECTION_NAME, person2);
        save(MONGO_DB_NAME, MONGO_COLLECTION_NAME, person3);
        person1.put("name","Danilo da Silva");
        findByCpf(MONGO_DB_NAME, MONGO_COLLECTION_NAME, "00012345622");
        update(MONGO_DB_NAME, MONGO_COLLECTION_NAME, person1);
        findByCpf(MONGO_DB_NAME, MONGO_COLLECTION_NAME, "00012345622");
        deleteByCpf(MONGO_DB_NAME,MONGO_COLLECTION_NAME,"00020003300");
        findByCpf(MONGO_DB_NAME, MONGO_COLLECTION_NAME, "00020003300");
    }

    public static void save(String dbName, String collectionName, BasicDBObject object ){
        DB db = getMongoClient().getDB(dbName);
        DBCollection table = db.getCollection(collectionName);

        table.insert(object);
        logger.info("Object saved.");
    }

    public static void update(String dbName, String collectionName, BasicDBObject object ){
        DB db = getMongoClient().getDB(dbName);
        DBCollection table = db.getCollection(collectionName);

        BasicDBObject query = new BasicDBObject();
        query.put("cpf", object.get("cpf"));

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", object);

        table.update(query, updateObj);
        logger.info("Object updated.");
    }

    public static void findByCpf(String dbName, String collectionName, String cpf ){
        DB db = getMongoClient().getDB(dbName);
        DBCollection table = db.getCollection(collectionName);

        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("cpf", cpf);

        DBCursor cursor = table.find(searchQuery);

        if (cursor.size() <= 0){
            logger.warning("No Results for " + cpf);
        }else {
            logger.info(cursor.size() + " Results");
        }
        while (cursor.hasNext()) {
            logger.info(cursor.next().toString());
        }
    }

    public static void deleteByCpf(String dbName, String collectionName, String cpf ){
        DB db = getMongoClient().getDB(dbName);
        DBCollection table = db.getCollection(collectionName);

        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("cpf", cpf);

        table.remove(searchQuery);
        logger.info("Object deleted.");
    }
}
