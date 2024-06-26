package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    
    String friends;
    protected void setup(Context context) throws IOException, InterruptedException {
        friends = new String();
    }
    
    @Override
    
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        for(Text localFriends: values)
            if(friends.length() != 0)
                friends += " " + localFriends.toString();
            else
                friends += localFriends.toString();
                
        context.write(new Text(friends), NullWritable.get());
    }
}
