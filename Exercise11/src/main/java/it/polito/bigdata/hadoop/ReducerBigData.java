package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Average,    // Input value type
                Text,           // Output key type
                Average> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<Average> values, // Input value type
        Context context) throws IOException, InterruptedException {
            Average globalAverage = new Average();
            globalAverage.setCount(0);
            globalAverage.setSum(0);

            for(Average localAvg: values){
                globalAverage.setCount(globalAverage.getCount() + localAvg.getCount());
                globalAverage.setSum(globalAverage.getSum() + localAvg.getSum());
            }

            context.write(key, globalAverage);
        
    }
}
