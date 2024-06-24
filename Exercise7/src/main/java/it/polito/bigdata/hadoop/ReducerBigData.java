package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        // Iterate over the set of values and sum them 
        String sentenceIdsConcatenated = new String();

        for (Text sentenceId: values){
            if(sentenceIdsConcatenated.length() == 0)
                sentenceIdsConcatenated = sentenceId.toString();
            else
                sentenceIdsConcatenated += "," + sentenceId.toString();
        }

        // emit the pair (word, sentenceIds)
        context.write(key, new Text(sentenceIdsConcatenated));

        
    }
}
