package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.Average;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Average> {// Output value type
    
    HashMap<String, Average> statistics;

    protected void setup(Context context) throws IOException, InterruptedException {
        statistics = new HashMap<String, Average>();
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String [] fields = value.toString().split(",");

            String sensorId = fields[0];
            float pm10Value = Float.parseFloat(fields[2]);

            if(statistics.containsKey(sensorId)){
                Average avgTemp = statistics.get(sensorId);
                avgTemp.setSum(avgTemp.getSum() + pm10Value);
                avgTemp.setCount(avgTemp.getCount() + 1);
                statistics.put(sensorId, avgTemp);
            }
            else{
                Average avgTemp = new Average();
                avgTemp.setCount(1);            
                avgTemp.setSum(pm10Value);
                statistics.put(sensorId, avgTemp);
            }    
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String sensorId: statistics.keySet())
            context.write(new Text(sensorId), statistics.get(sensorId));
    }
}
