package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.DateIncome;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    DateIncome> {// Output value type
    
    DateIncome mostProfitableIncome;

    protected void setup(Context context) throws IOException, InterruptedException {
        mostProfitableIncome = new DateIncome();
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            float income = Float.parseFloat(value.toString());

            if(income > mostProfitableIncome.getIncome()){
                mostProfitableIncome.setDate(key.toString());
                mostProfitableIncome.setIncome(income);
                context.write(NullWritable.get(), mostProfitableIncome);
            }
            
            
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), mostProfitableIncome);
    }
}
