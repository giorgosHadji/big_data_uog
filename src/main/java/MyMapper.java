

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.PorterStemmer;

public class MyMapper extends Mapper<LongWritable, Text, CustomKey, FreqDocPair> {


	private CustomKey keyPair = new CustomKey();//the key output
	private	FreqDocPair freqdoc=new FreqDocPair();//the value output
	private static PorterStemmer stemmer = new PorterStemmer();

	static enum Counters_index {//we need to count the number of tokens to find the length of the document
		NUM_TOKENS
	}
	HashSet<String> stopWords = new HashSet<String>();

	@Override
	//In this function we load the stopwords needed for removing while tokenizing
	//it is executed only once per mapper for initilization
	protected void setup(Context context) throws IOException, InterruptedException {

		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles != null && cacheFiles.length > 0) {
			try {
				String line = "";
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path getFilePath = new Path(cacheFiles[0].toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
				while ((line = reader.readLine()) != null) {
					String[] words = line.split(" ");

					for (int i = 0; i < words.length; i++) {
						// add the words to ArrayList
						stopWords.add(words[i].toLowerCase());
					}
				}
			}

			catch (Exception e) {
				System.out.println("Unable to read the File");
				System.exit(1);
			}
		}
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		HashMap<String, Long> tf = new HashMap<>();
		ArrayList<String> tokens = new ArrayList<>();
		int count = 0;
		//removing unneccesary symbols and splitting into tokens
		String[] words = value.toString().toLowerCase().replaceAll("[^a-z]", " ").split("\\s+");
		//stem the words
		for (String term : words) {
			term=term.trim();

			if (stopWords.contains(term) || term.equals(""))
				continue;
			char word[] = term.toCharArray();
			stemmer.add(word, word.length);
			stemmer.stem();
			tokens.add(stemmer.toString());
		}
		//count the frequency of each token
		for (String term : tokens) {
			count++;
			if (tf.containsKey(term))
				tf.put(term, tf.get(term) + 1);
			else {
				tf.put(term,(long) 1);
			}
		}
		//emit the key-value pair
		for (Map.Entry<String, Long> entry : tf.entrySet()) {
			keyPair.set(entry.getKey(), entry.getValue());//term with frequency as key
			freqdoc.set(Long.parseLong(key.toString()), entry.getValue());//doc id with frequency as value
			context.write(keyPair, freqdoc);
		}
		//here we output the document length using a special key to recognize it.
		keyPair.set("___",-Long.parseLong(key.toString()));//special key 
		freqdoc.set(Long.parseLong(key.toString()), (long)count);//doc id and its length

		context.write(keyPair, freqdoc);
		context.getCounter(Counters_index.NUM_TOKENS).increment(count);//increase with this document length
	}

}