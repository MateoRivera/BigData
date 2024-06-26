package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateIncome {
	private String date;
	private float income;

	public DateIncome(String date, float income){
		this.date = date;
		this.income = income;
	}

	public DateIncome(DateIncomeWritable parse){
		this.date = parse.getDate();
		this.income = parse.getIncome();
	}

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

	public String toString() {
		String formattedString = new String(date + "\t" + income);

		return formattedString;
	}

}
