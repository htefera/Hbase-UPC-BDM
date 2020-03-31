package bdm.labs.hbase.writer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import adult.avro.Adult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

import static java.lang.System.exit;

public class MyHBaseWriter implements MyWriter {

    private Configuration config;
    private Connection connection;

    protected int key;

    protected HashMap<String,String> data;

    protected BufferedMutator buffer;

    public MyHBaseWriter() {
        this.key = 0;
        this.reset();
    }

    public void open(String tableName) throws IOException {
        this.config = HBaseConfiguration.create();
        config.set("hadoop.security.authentication", "simple");
        config.set("hadoop.security.authorization","false");
        config.set("hbase.security.authentication", "simple");
        config.set("hbase.security.authorization","false");
        //config.set("hbase.zookeeper.quorum","magmar.fib.upc.es:2181");
        config.set("hbase.zookeeper.quorum", "host");
        //config.set("hbase.zookeeper.property.clientPort", "2181");

        this.connection = ConnectionFactory.createConnection(this.config);
        this.buffer = this.connection.getBufferedMutator(TableName.valueOf(tableName));
    }

    protected String nextKey() {

        return String.valueOf(this.key);
    }

    protected String toFamily(String attribute) {
        //create 'Training2', 'demo', 'qualities', 'others'
        String family = "";

        switch(attribute) {
            case "age":
            case "maritalStatus":
            case "relationship":
            case "race":
            case "sex":
            case "nativeCountry":
                family = "demo";
                break;
            case "education":
            case "educationNum":
            case "workingClass":
                family = "qualities";
                break;
            case "fnlwgt":
            case "capitalGain":
            case "capitalLoss":
            case "hoursPerWeek":
            default:
                family = "others";
        }
        //System.out.println(attribute + "-" + family);
        //exit(0);
        return family;
    }

    public void put(Adult a) {
        data.put("age", a.getAge().toString());
        data.put("workingClass", a.getWorkclass().toString());
        data.put("fnlwgt", a.getFnlwgt().toString());
        data.put("education", a.getEducation().toString());
        data.put("educationNum", a.getEducationNum().toString());
        data.put("maritalStatus", a.getMaritalStatus().toString());
        data.put("relationship", a.getRelationship().toString());
        data.put("race", a.getRace().toString());
        data.put("sex", a.getSex().toString());
        data.put("capitalGain", a.getCapitalGain().toString());
        data.put("capitalLoss", a.getCapitalLoss().toString());
        data.put("hoursPerWeek", a.getHoursPerWeek().toString());
        data.put("nativeCountry", a.getNativeCountry().toString());
    }

    public void reset() {
        data = new HashMap<String, String>();
    }

    public int flush() throws IOException {
        String rowKey = this.nextKey();
        System.out.println("Row with key "+rowKey+" outputted");

        // Create a new Put object with an incremental key
        //Put put = new Put(rowKey.getBytes());
        String myKey = data.get("nativeCountry");
        Put put = new Put(myKey.getBytes());

        // Now get all the columns
        Set<Entry<String,String>> entries = this.data.entrySet();
        int length = 0;
        for (Entry<String, String> entry : entries) {
            // Add the value in the Put object
            String attribute = entry.getKey();
            String family = this.toFamily(attribute);
            String value = entry.getValue();
            put.addColumn(family.getBytes(), attribute.getBytes(), value.getBytes());

            length += value.length();
        }
        // Insert it!
        this.buffer.mutate(put);

        this.key++;
        this.reset();
        return length;
    }

    public void close() throws IOException {
        this.buffer.flush();
        this.buffer.close();
        this.connection.close();
    }

}
