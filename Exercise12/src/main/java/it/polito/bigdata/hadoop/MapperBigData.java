package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type

    float threshold;

    protected void setup(Context context) throws IOException, InterruptedException {
        threshold = Float.parseFloat(context.getConfiguration().get("threshold"));
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            float pm10Value = Float.parseFloat(value.toString());

            if(pm10Value < threshold)
                context.write(key, new FloatWritable(pm10Value));
            
    }
}
