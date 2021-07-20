package com.coltonjenkins.java.telemtry;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import lombok.extern.slf4j.*;

@Slf4j
public class ProducerApp {
    private static final String SOURCE = "input-stream";
    private static final String SERVER_URL = "kafka:9092";
    private static final String APP_D_CUST_ID = "1234";
    private static final Integer CPU_POLL_INTERVAL = 5000; // in milli
    private static final Integer CLK_TCK = 100; // getconf CLK_TCK

    public static void main(String[] args) {
      // create instance for properties to access producer configs   
      var props = new Properties();
      
      //Assign localhost id
      props.put("bootstrap.servers", SERVER_URL);
      
      //Set acknowledgements for producer requests.      
      props.put("acks", "all");
      
      //If the request fails, the producer can automatically retry,
      props.put("retries", 0);
      
      //Specify buffer size in config
      props.put("batch.size", 16384);
      
      //Reduce the no of requests less than 0   
      props.put("linger.ms", 1);
      
      //The buffer.memory controls the total amount of memory available to the producer for buffering.   
      props.put("buffer.memory", 33554432);
      
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
         
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.IntegerSerializer");
      
      var producer = new KafkaProducer<String, Integer>(props);
            
      // Run indefinite for demo
      for(var i = 0; i<10; i++) {
        var cpuLoad = getCpuLoad();
        producer.send(new ProducerRecord<>(SOURCE, APP_D_CUST_ID + "-cpuAvg", cpuLoad));
        log.info("Message sent successfully");
        log.info("CPU AVG is: " + cpuLoad);

        try { Thread.sleep(CPU_POLL_INTERVAL); } 
        catch(InterruptedException ex) { 
            ex.printStackTrace(); 
            Thread.currentThread().interrupt();
            System.exit(1); 
        }
      }

      producer.close();
    } 

    public static Integer getCpuLoad() {
        try(var cpuReader = new BufferedReader(new InputStreamReader(new FileInputStream("/proc/stat")))) {
            var cpuMetrics = Arrays.asList(cpuReader.readLine().split("\\s+"));
            var totalTime = Integer.valueOf(cpuMetrics.get(1)) + Integer.valueOf(cpuMetrics.get(2)) + Integer.valueOf(cpuMetrics.get(3));

            return totalTime/CLK_TCK; // need to also take into account bTime, but for brevity leaving as is
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    
        return 0;
    }
}