


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//class to implement the value of the mapper output
//it has as attributes a document id and the corresponding frequency of a term in that document
public class FreqDocPair implements WritableComparable<FreqDocPair> {

	public Long doc_id;
	public Long freq;

	public FreqDocPair() {
	}

	public FreqDocPair(Long doc_id, Long freq) {
		super();
		this.set(doc_id, freq);
	}

	public void set(Long doc_id, Long freq) {
		this.doc_id = doc_id;
		this.freq = freq;
	}
	//the following methods are necessary to implement in order to be used in MapReduce
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(doc_id);
		out.writeLong(freq);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		doc_id = in.readLong();
		freq = in.readLong();
	}

	@Override
	public int compareTo(FreqDocPair o) {
		  int cityCmp = freq.compareTo(o.freq);
			if (cityCmp != 0) {
				return -cityCmp;
			} else {
				return Long.compare(doc_id, o.doc_id);
			}
		}
	

}
