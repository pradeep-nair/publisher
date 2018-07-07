package com.appdynamics.publisher;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Publisher {

    /**
     * This method loads the config YAML file
     *
     * @param   path    Path to config file
     * @return  A {@code Map<String, Map<String, ?>>} containing config key value pairs
     */
    private static Map<String, Map<String, ?>> load(String path) {
        Yaml yml = new Yaml();
        try (BufferedReader ip = new BufferedReader(new FileReader(path))) {
            return yml.load(ip);
        } catch (IOException ioe) {
            System.out.println("error loading Yaml!!");
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * This method connects to a database and executes the select query
     *
     * @param   url   Database url
     * @param   props Connection properties
     * @param   query Query to execute
     * @return  A {@code List<List<String>>} containing the result of the query
     */
    private static List<List<String>> executeQuery(String url, Properties props, String query) {
        List<List<String>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int colSize = rsmd.getColumnCount();
            while (rs.next()) {
                List<String> tuple = new ArrayList<>();
                for (int i = 1; i <= colSize; i++) tuple.add(rs.getString(i));
                result.add(tuple);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * This method connects to RabbitMQ server and publishes the result as message\
     *
     * @param   host        host IP of the RabbitMQ server
     * @param   queueName   name of the queue
     * @param   message     message to publish
     */
    private static void publish(String host, String queueName, byte[] message) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // connects to a broker (AMQConnection)
        try (com.rabbitmq.client.Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // declare a queue
            channel.queueDeclare(queueName, false, false, false, null);
            while (true) {
                channel.basicPublish("", queueName, null, message);
            }
//            System.out.println(" [x] Sent '" + message + "'");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns byte representation of list
     *
     * @param   list    Object to be serialized
     * @return  A {@code byte[]} representation of list object
     */
    private static byte[] serialize(List<List<String>> list) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream os = new ObjectOutputStream(bos)){
            os.writeObject(list);
            return bos.toByteArray();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        // load config yaml
        Map<String, Map<String, ?>> config = load("/Users/pradeep.nair/repos/appdynamics/" +
                "extensions/publisher/src/main/resources/conf/config.yml");
        // get postgres connection properties
        Map<String, ?> psqlProps = config.get("postgres_config");
        String host = (String) psqlProps.get("host");
        Integer port = (Integer) psqlProps.get("port");
        String database = (String) psqlProps.get("database");
        String user = (String) psqlProps.get("user");
        String password = (String) psqlProps.get("password");
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        // execute query
        List<List<String>> result = executeQuery(url, props, "SELECT * FROM student");
        // publish message to RabbitMQ
        Map<String, ?> rabbitProps = config.get("rabbit_config");
        String rabbitHost = (String) rabbitProps.get("host");
        String queueName = (String) rabbitProps.get("queueName");
        byte[] message = serialize(result);
        publish(rabbitHost, queueName, message);
    }
}
