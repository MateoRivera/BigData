package it.polito.bigdata.hadoop;

import java.io.IOException;

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
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
        float min = 0;
        float max = 0;
        int i = 0;

        // Iterate over the set of values and sum them 
        for (FloatWritable value : values) {
            if (i==0){
                min = value.get();
                max = value.get();
                i++;
                continue;
            }

            if(value.get() < min)
                min = value.get();

            if(value.get() > max)
                max = value.get();
        }

        String result = "max=" + String.valueOf(max) + "_min=" + String.valueOf(min);

        context.write(key, new Text(result));
    }
}
