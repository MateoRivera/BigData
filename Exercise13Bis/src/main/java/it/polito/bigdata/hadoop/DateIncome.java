package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateIncome implements 
org.apache.hadoop.io.Writable {
	private String date = "";
	private float income = 0;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public float getIncome() {
		return income;
	}

	public void setIncome(float income) {
		this.income = income;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = in.readUTF();
		income = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeFloat(income);
	}

	public String toString() {
		String formattedString = new String(date + "\t" + income);

		return formattedString;
	}

}
