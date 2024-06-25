package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    private MultipleOutputs<Text, NullWritable> mos;

    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String [] fields = value.toString().split(",");
            float temperature = Float.parseFloat(fields[3]);

            if(temperature <= 30)
                mos.write("normaltemp", value, NullWritable.get());
            else
                mos.write("hightemp", value, NullWritable.get());
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
