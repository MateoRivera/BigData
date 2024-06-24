package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type

    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

           String [] fields = value.toString().split(",");

           String date = fields[1];
           float temperature = Float.parseFloat(fields[3]);

           context.write(new Text(date), new FloatWritable(temperature));
            
    }

   
}
