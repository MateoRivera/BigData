package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.avro.JsonProperties.Null;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                DateIncome,    // Input value type
                NullWritable,           // Output key type
                DateIncome> {  // Output value type
    
    @Override
    
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<DateIncome> values, // Input value type
        Context context) throws IOException, InterruptedException {
        DateIncome mostProfitableIncome = new DateIncome();

        for(DateIncome localProfitableIncome: values){
            if(localProfitableIncome.getIncome() > mostProfitableIncome.getIncome()){
                mostProfitableIncome.setDate(localProfitableIncome.getDate());
                mostProfitableIncome.setIncome(localProfitableIncome.getIncome());
            }
        }
        context.write(NullWritable.get(), mostProfitableIncome);
        
    }
}
