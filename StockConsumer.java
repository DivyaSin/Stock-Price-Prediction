package lab2.lab2;

import org.apache.kafka.clients.consumer.*;


import java.io.IOException;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StockConsumer {
    // Declare a new consumer
    public static KafkaConsumer<String, JsonNode> consumer;

    public static void main(String[] args) throws IOException {
    	
        // check command-line arguments
        if(args.length != 5) {
            System.err.println("usage: StockConsumer <broker-socket> <input-topic> <stock-symbol> <group-id> <threshold-%>");
            System.err.println("e.g.: StockConsumer localhost:9092 stats orcl mycg 0.5");
            System.exit(1);
        }   
        
        // initialize varaibles
        String brokerSocket = args[0];
        String inputTopic = args[1];
        String stockSymbol = args[2];
        String groupId = args[3];
        double thresholdPercentage = Double.parseDouble(args[4]);     
        long pollTimeOut = 1000;  
        double previousAggregatedStatistic = 0, currentAggregatedStatistic = 0;
        
        // configure consumer
        configureConsumer(brokerSocket, groupId);
        
        // TODO subscribe to the topic
        List<String> topics = new ArrayList<>();
        topics.add(inputTopic);
        consumer.subscribe(topics);       
        String action = "hold";
        
        // TODO loop infinitely -- pulling messages out every pollTimeOut ms
        while(true){       	
        	// TODO iterate through message batch
        	ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(pollTimeOut);
        	Iterator<ConsumerRecord<String, JsonNode>> iterator = consumerRecords.iterator();
        	
            double sumMeanHigh = 0, sumMeanLow = 0, sumMeanOpen = 0, sumMeanClose = 0, lastClose=0;
            long sumMeanVolume = 0;
            int numOfRecords = 0;
            String timestamp = null;
            
        	while(iterator.hasNext()){
        		
        		// TODO create a ConsumerRecord from message
        		ConsumerRecord<String, JsonNode> record = iterator.next(); 
        		
        		// TODO pull out statistics from message
        		timestamp = record.value().get("lastTimestamp").asText();
        		sumMeanHigh += record.value().get("meanHigh").asDouble();
        		sumMeanLow += record.value().get("meanLow").asDouble();
        		sumMeanOpen += record.value().get("meanOpen").asDouble();
        		sumMeanClose += record.value().get("meanClose").asDouble();
        		sumMeanVolume += record.value().get("meanVolume").asLong();
                lastClose = record.value().get("lastClose").asDouble();
                numOfRecords ++;

            }
            if(numOfRecords > 0){
            	
            	// TODO calculate batch statistics meanHigh, meanLow, meanOpen, meanClose, meanVolume
                double meanMeanHigh = 0, meanMeanLow = 0, meanMeanOpen = 0, meanMeanClose = 0, meanMeanVolume = 0;
                meanMeanHigh = sumMeanHigh/numOfRecords;
                meanMeanLow = sumMeanLow/numOfRecords;
                meanMeanOpen = sumMeanOpen/numOfRecords;
                meanMeanClose = sumMeanClose/numOfRecords;
                meanMeanVolume = sumMeanVolume/numOfRecords;
            		
             // TODO calculate currentAggregatedStatistic and compare to previousAggregatedStatistic
        		currentAggregatedStatistic = meanMeanVolume * (meanMeanHigh + meanMeanLow + meanMeanOpen + meanMeanClose) / 4.0;
        		double deltaPercentage = (currentAggregatedStatistic - previousAggregatedStatistic)/(100 * meanMeanVolume);
        		
        		if(Math.abs(deltaPercentage) > thresholdPercentage){			
        			// TODO determine if delta percentage is greater than threshold
        			if(deltaPercentage > 0){
        				action = "sell";
                    }
        			else{
        				action = "buy";
                    }
        		}
        		else{
        			action = "hold";     
                }
        		
        		// TODO print output to screen
        		System.out.println(timestamp+","+stockSymbol+","+lastClose+","+deltaPercentage+","+action);    		
        		previousAggregatedStatistic = currentAggregatedStatistic;
            }
        }        
    }
    public static void configureConsumer(String brokerSocket, String groupId) {
        Properties props = new Properties();
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", brokerSocket);
        props.put("group.id", groupId);
        props.put("auto.commit.enable", true);

        consumer = new KafkaConsumer<String, JsonNode>(props);
    }
}