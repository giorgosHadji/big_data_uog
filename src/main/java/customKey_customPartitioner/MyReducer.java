package customKey_customPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<CustomKey, FreqDocPair, Text, Text> {
	private MultipleOutputs mos;
	private	StringBuilder sb;
	private	Long doc;
	private	Text value=new Text();
	private LongWritable result = new LongWritable();
	//initiliaze some variables we will need.
	public void setup(Context context) {
		mos = new MultipleOutputs(context);//is needed to write to different outputs
		sb = new StringBuilder();
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();//when finished close the connection 
	}

	@Override
	protected void reduce(CustomKey key, Iterable<FreqDocPair> values, Context context) throws IOException, InterruptedException {


		if (key.key.equals("___")) {// here we print the document_id -> document length			
			for (FreqDocPair val:values) {
				doc = val.doc_id;
				result.set(val.freq);
				mos.write("docToLen", doc, result,"docToLen/docToLen");//save it to the directory docToLen
			}
		} else {
			//We use a string builder as it is faster if you append many times in respecti to "+" operator
			sb.append("{");
			//here we create the posting list for each term
			for (FreqDocPair val:values) {
				sb.append(val.doc_id.toString());
				sb.append(":");
				sb.append(val.freq.toString());
				sb.append(",");
			}
			sb.deleteCharAt(sb.length() - 1 );
			sb.append("}");

		}			
		value.set(sb.toString());
		mos.write("postingList", key.key,value,"postingList/postingList");
		sb.setLength(0);
	}


}
