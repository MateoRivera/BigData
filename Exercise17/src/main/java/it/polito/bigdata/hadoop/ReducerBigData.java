package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
            int counter = 0;
            float maxTemperature = 0;

            for(FloatWritable temperature: values){
                if(counter == 0){
                    maxTemperature = temperature.get();
                    counter ++;
                    continue;
                }
                
                if(temperature.get()>maxTemperature)
                    maxTemperature = temperature.get();
                    
            }

            context.write(key, new FloatWritable(maxTemperature));
    }
}
