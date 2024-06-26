package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
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
                    Text,         // Output key type
                    NullWritable> {// Output value type

    ArrayList<String> stopWords;
    protected void setup (Context context) throws IOException, InterruptedException {
        stopWords = new ArrayList<String>();

        String line;
        URI[] urisCachedFiles = context.getCacheFiles();

        BufferedReader file = new BufferedReader(
            new FileReader(
                new File(
                    urisCachedFiles[0].getPath()
                )
            )
        );

        while((line = file.readLine()) != null)
            stopWords.add(line);
        
        file.close();
    }

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");
            String lineWithoutStopWords = "";
            // Iterate over the set of words
            for(String word : words)                
                if(!stopWords.contains(word.toLowerCase()))
                    if(lineWithoutStopWords.length() != 0)
                        lineWithoutStopWords += " "+ word;
                    else
                        lineWithoutStopWords += word;

            context.write(new Text(lineWithoutStopWords), NullWritable.get());
    }
}
