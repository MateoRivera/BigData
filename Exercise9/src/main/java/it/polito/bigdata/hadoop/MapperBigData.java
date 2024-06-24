package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    HashMap<String, Integer> wordCounter;

    protected void setup(
            Context context)
            throws IOException, InterruptedException {
                wordCounter = new HashMap<String, Integer>();
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String [] words = value.toString().split("\\s+");
            for(String word: words){
                String cleanedWord = word.toLowerCase();

                if(wordCounter.containsKey(cleanedWord))
                    wordCounter.put(cleanedWord, wordCounter.get(cleanedWord)+1);
                else
                    wordCounter.put(cleanedWord, 1);
            
            }
    }

    protected void cleanup(
            Context context)
            throws IOException, InterruptedException {
                for(String word: wordCounter.keySet())
                    context.write(new Text(word), new IntWritable(wordCounter.get(word)));
    }
}
