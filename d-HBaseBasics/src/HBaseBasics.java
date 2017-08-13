
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseBasics {
  public static void main(String[] args) throws IOException
  {
    HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
    HTableDescriptor htable = new HTableDescriptor("User"); 
    htable.addFamily( new HColumnDescriptor("Id"));
    htable.addFamily( new HColumnDescriptor("Name"));
    System.out.println( "Connecting..." );
    HBaseAdmin hbase_admin = new HBaseAdmin( hconfig );
    System.out.println( "Creating Table..." );
    hbase_admin.createTable( htable );
    System.out.println("Done!\n");
    
    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, "User");
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("AAA"));
    p.add(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("BBB"));
    System.out.println("Populating the table...");
    table.put(p);
    System.out.println("Done!\n");
    
    Get g = new Get(Bytes.toBytes("row1"));
    Result r = table.get(g);
    byte [] value = r.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col1"));
    byte [] value1 = r.getValue(Bytes.toBytes("Name"),Bytes.toBytes("col2"));
    String valueStr = Bytes.toString(value);
    String valueStr1 = Bytes.toString(value1);
    System.out.println("Showing GET result\n" +"Id: "+ valueStr+"\n"+"Name: "+valueStr1+"\n");
    
    org.apache.hadoop.conf.Configuration config2 = HBaseConfiguration.create();
    HTable table2 = new HTable(config2, "User");
    Put p2 = new Put(Bytes.toBytes("row1"));    
    p2.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("CCCC"));
    p2.add(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("DDDD"));
    System.out.println("Updating the table...");
    table2.put(p2);
    System.out.println("Done!\n");
    
    Get g2 = new Get(Bytes.toBytes("row1"));
    Result r2 = table.get(g2);    
    byte [] value3 = r2.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col1"));
    byte [] value4 = r2.getValue(Bytes.toBytes("Name"),Bytes.toBytes("col2"));
    String valueStr3= Bytes.toString(value3);
    String valueStr4 = Bytes.toString(value4);
    System.out.println("Showing GET result\n" +"Id: "+ valueStr3+"\n"+"Name: "+valueStr4+"\n");   
        
    System.out.println("Deleting the table...");
    hbase_admin.disableTable("User");
    hbase_admin.deleteTable("User");
    System.out.println("Done!\n");      
  }
}
