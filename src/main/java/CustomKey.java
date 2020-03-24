


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//class to implement the composite key.
//It has as attributes the term and the frequency of that term in the document.

public class CustomKey implements WritableComparable<CustomKey> {

	public String key=new String();//for doc - length the key is "___"
	public Long freq;

	public CustomKey() {
	}

	public CustomKey(String key, Long freq) {
		super();
		this.set(key, freq);
	}

	public void set(String key, Long freq) {
		this.key =key;
		this.freq = freq;
	}
	//the following methods are necessary to implement in order to be used in MapReduce

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeLong(freq);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		freq = in.readLong();
	}

	@Override
	public int compareTo(CustomKey o) {
		int stateCmp = key.compareTo(o.key);
		if (stateCmp != 0) {
			return stateCmp;
		} else {
			int cityCmp = freq.compareTo(o.freq);
			return -cityCmp;//use minus in order to get descending sorting

		}
	}

}
