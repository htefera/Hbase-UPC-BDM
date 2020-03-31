package bdm.labs.hbase.reader;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHBaseReader implements MyReader {

	private Configuration config;
	private Connection connection;
	private Table table;
	
	private ResultScanner scanner;
	
	public MyHBaseReader() {}
	
	protected String scanStart(String first) {
		return first;
	}
	
	protected String scanStop(String last) {
		return last;
	}
	
	protected String[] scanFamilies() {
		return null;
	}
	
	public void open(String tableName) throws IOException {
		this.config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum","host");
		this.connection = ConnectionFactory.createConnection(config);
		this.table = this.connection.getTable(TableName.valueOf(tableName));
		
		String start = this.scanStart("10");
		String stop = this.scanStop("20");
		String[] families = this.scanFamilies();

		Scan scan = new Scan();
		if (start != null) {
			scan.setStartRow(start.getBytes());

		}
		if (stop != null) {
			scan.setStopRow(stop.getBytes());
		}
		if (families != null) {
			for (String family : families) {
				scan.addFamily(family.getBytes());
			}
		}
		this.scanner = this.table.getScanner(scan);
	}
	
	public String next() throws IOException {
		// Get next row
		Result next = this.scanner.next();
		if (next != null) {
			// First get the row key
			StringBuilder line = new StringBuilder(new String(next.getRow())+'\t');
			boolean first = true;
			
			// Now get all the families
			Set<byte[]> families = next.getMap().keySet();
			for (byte[] family : families) {
				// For each family, get all the qualifiers
				Set<byte[]> qualifiers = next.getFamilyMap(family).keySet();
				for (byte[] qualifier : qualifiers) {
					// Get the value (most recent version)
					byte[] value = next.getValue(family, qualifier);
					if (first) {
						line.append(new String(qualifier)+':'+new String());
						first = false;
					}
					else {
						line.append(','+new String(qualifier)+':'+new String(value));
					}
				}
			}
			return line.toString();
		}
		return null;
	}
	
	
	public void close() throws IOException {
		this.scanner.close();
		this.table.close();
		this.connection.close();
	}
	
}
