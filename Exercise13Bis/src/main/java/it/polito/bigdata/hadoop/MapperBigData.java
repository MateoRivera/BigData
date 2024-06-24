package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
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
    
    ArrayList<DateIncome> topK;
    int k;

    protected void setup(Context context) throws IOException, InterruptedException {
        k = Integer.parseInt(context.getConfiguration().get("k"));
        topK = new ArrayList<DateIncome>();
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
        DateIncome currentDateIncome = new DateIncome();
        currentDateIncome.setDate(key.toString());
        currentDateIncome.setIncome(Float.parseFloat(value.toString()));

        for(int i=0; i < topK.size(); i++){
            if(currentDateIncome.getIncome() > topK.get(i).getIncome()){
                topK.add(i, currentDateIncome);
            
                if(topK.size() == k+1)
                    topK.remove(k);
                
                return;
            }
        }

        if(topK.size() < k)
            topK.add(currentDateIncome);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(DateIncome e: topK)
            context.write(NullWritable.get(), e);
    }
}
