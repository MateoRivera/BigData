package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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
    
    int k;
    HashMap<Integer, DateIncome> topK;
    
                
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = Integer.parseInt(context.getConfiguration().get("k"));
        topK = new HashMap<Integer, DateIncome>();
    }

    protected void reduce(
        NullWritable key, // Input key type
        Iterable<DateIncome> values, // Input value type
        Context context) throws IOException, InterruptedException {

        for(DateIncome currentDateIncome: values){
            boolean wasCurrentDateIncomeAdded = false;

            for(int i = 0; i < topK.size(); i++)              

                if(currentDateIncome.getIncome() > topK.get(i).getIncome()){                    
                    insert(topK, i, currentDateIncome);
                    wasCurrentDateIncomeAdded = true;
                    
                    if(topK.size() == k+1){
                        topK.remove(k);
                    }
                    break;
                }

                
            if(topK.size() < k && !wasCurrentDateIncomeAdded)
                insert(topK, -1, currentDateIncome);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(DateIncome e: topK.values())
            context.write(NullWritable.get(), e);
    }

    private void insert(HashMap<Integer, DateIncome> h, int index, DateIncome e){

        DateIncome toInsert = e;
        DateIncome aux;
        
        if(index != -1)
            for(int i = index; i < h.size(); i++){
                aux = h.get(i);
                h.put(i, toInsert);

                toInsert = aux;
            }

        h.put(h.size(), toInsert);
    }
}
