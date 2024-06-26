package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    private String userOfInterest;
    private String friends;

    protected void setup(Context context) throws IOException, InterruptedException {
        userOfInterest = context.getConfiguration().get("userOfInterest");
        friends = new String();
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
        
        String [] users = value.toString().split(",");
        if(userOfInterest.equalsIgnoreCase(users[0]))
            if(friends.length() != 0)
                friends += " " + users[1];
            else
                friends += users[1];

        else if(userOfInterest.equalsIgnoreCase(users[1]))
            if(friends.length() != 0)
                friends += " " + users[0];
            else
                friends += users[0];
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(friends));
    }
}
