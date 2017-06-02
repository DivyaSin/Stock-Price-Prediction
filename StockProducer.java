package lab2.lab2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.lang.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
        
public class StockProducer {
	
    // Set the stream and topic to publish to.
    public static String topic;
       
    // Declare a new producer
    public static KafkaProducer<String, JsonNode> producer;       
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {

    // check command-line args
    if(args.length != 5) {
    	System.err.println("usage: StockProducer <broker-socket> <input-file> <stock-symbol> <output-topic> <sleep-time>");
        System.err.println("eg: StockProducer localhost:9092 /user/user01/LAB2/orcl.csv orcl prices 1000");
        System.exit(1);
    }
        
        // initialize variables
    String brokerSocket = args[0];
    String inputFile = args[1];
    String stockSymbol = args[2];
    String outputTopic = args[3];
    long sleepTime = Long.parseLong(args[4]); 
        
    // configure the producer
    configureProducer(brokerSocket);    
        
    // TODO create a buffered file reader for the input file
    try (BufferedReader br = new BufferedReader(new FileReader(new File(inputFile)))) {
    	String currentLine ;
    	
    	// TODO loop through all lines in input file
    	while ((currentLine = br.readLine()) != null) {
    		String[] dataList = currentLine.split(","); 
                
            // TODO filter out "bad" records
    		if (dataList.length != 7){
    			continue;
    		} 
//                boolean isValid = true;
                
    		// check dateformat is valid
    		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    		Date date;
    		try {
    			date = dateFormat.parse(dataList[0]);
    		} 
    		catch (ParseException e) {
//                    isValid = false;
//                	System.out.println("\nSkipped\n");
    			continue;
    		}
                
    		// check if data is valid
    		int [] indices = {1, 2, 3, 4, 5, 6};
    		for (int i: indices){
    			Double value;
    			try{
    				value = Double.parseDouble(dataList[i]);
    			}
    			catch (NumberFormatException e) {
//                        isValid = false;
//                        break;
    				continue;
    			}
    		}
               /* if (!isValid) {
                    continue;
                }*/
                
    		// TODO create an ObjectNode to store data in
    		JsonNodeFactory factory = JsonNodeFactory.instance;
                
    		// ObjectMapper mapper = new ObjectMapper(); // can use jsonnodefactory 
    		ObjectNode objNode = factory.objectNode();
                
    		// TODO parse out the fields from the line and create key-value pairs in ObjectNode
    		objNode.put("timestamp", dataList[0]);
    		objNode.put("open", dataList[1]);
    		objNode.put("high", dataList[2]);
    		objNode.put("low", dataList[3]);
    		objNode.put("close", dataList[4]);
    		objNode.put("volume", dataList[5]);
                
    		// System.out.println(objectNode);
    		ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(outputTopic, stockSymbol, objNode);
    		producer.send(record);
    		System.out.println(objNode);
                
    		// TODO sleep the thread
    		Thread.sleep(sleepTime);
    	}
            
    	// TODO close buffered reader
    	br.close(); 
    }
    catch (IOException e) {
    	e.printStackTrace();
    } 
    // TODO close producer
    producer.close();    
    }
    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, JsonNode>(props);
    }
}
