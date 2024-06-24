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
                DateIncomeWritable,    // Input value type
                NullWritable,           // Output key type
                DateIncomeWritable> {  // Output value type
    
    int k;
    ArrayList<DateIncome> topK;
    
                
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = Integer.parseInt(context.getConfiguration().get("k"));
        topK = new ArrayList<DateIncome>();
    }

    protected void reduce(
        NullWritable key, // Input key type
        Iterable<DateIncomeWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        for(DateIncomeWritable currentDateIncome: values){
            boolean wasCurrentDateIncomeAdded = false;

            for(int i = 0; i < topK.size(); i++)              

                if(currentDateIncome.getIncome() > topK.get(i).getIncome()){                    
                    topK.add(i, new DateIncome(currentDateIncome));
                    wasCurrentDateIncomeAdded = true;
                    
                    if(topK.size() == k+1)
                        topK.remove(k);
                    
                    break;
                }

                
            if(topK.size() < k && !wasCurrentDateIncomeAdded)
                topK.add(new DateIncome(currentDateIncome));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(DateIncome e: topK){
            DateIncomeWritable eWritable = new DateIncomeWritable();
            eWritable.setDate(e.getDate());
            eWritable.setIncome(e.getIncome());
            context.write(NullWritable.get(), eWritable);
        }
    }

    private HashMap<Integer, DateIncomeWritable> insert(HashMap<Integer, DateIncomeWritable> h, int index, DateIncomeWritable e){

        DateIncomeWritable toInsert = e;
        DateIncomeWritable aux;
        
        if(index != -1)
            for(int i = index; i < h.size(); i++){
                aux = h.get(i);
                h.put(i, toInsert);

                toInsert = aux;
            }

        h.put(h.size(), toInsert);

        return h;
    }
}
