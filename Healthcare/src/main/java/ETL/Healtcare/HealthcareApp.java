package ETL.Healtcare;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import ETL.Healtcare.*;

public class HealthcareApp {
	
	public static PaymentId createPaymentId(Payments p) {
		String id=p.getPhysicianId() + '_' + p.getDateOfPayment() + '_' + p.getRecordId();
		return new PaymentId(id, p.getPhysicianId(),p.getDateOfPayment(),p.getPayer(),p.getAmount(),
				p.getPhysicianSpeciality(),p.getNatureOfPayment());
	}

	public static void main(String[] args) {
		//Setting conf object
		SparkConf conf=new SparkConf();
		conf.setMaster("local[2]");
		
		//Starting Spark session
		SparkSession session=SparkSession.builder().config(conf).appName("HealthCareApp").getOrCreate();
		
		Dataset<Row> ds=session.read().option("header", true).csv("---source directory---/Projects/HealthcareETL/Dataset/health.csv");
		
		//ds.withColumn("amount", ds.col("Total_Amount_of_Payment_USDollars"));
		System.out.println(ds.first());
		ds.show();
		System.out.println("\n\n******************Custom Schema********************");
		ds.createOrReplaceTempView("payments");
		
		Dataset<Payments> paymentsDs=session.sql("select Physician_Profile_ID as physicianId, "
				+ "Date_of_Payment as dateOfPayment, Record_ID as recordId, "
				+ "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  "
				+ "Total_Amount_of_Payment_USDollars as amount, Physician_Specialty as physicianSpeciality, "
				+ "Nature_of_Payment_or_Transfer_of_Value as natureOfPayment from payments")
				.as(Encoders.bean(Payments.class));
		paymentsDs.show();
		
		paymentsDs.createOrReplaceTempView("payments");
		paymentsDs.printSchema();
		
		paymentsDs.groupBy("natureOfPayment").count().orderBy("count").show();
		paymentsDs.write().csv("file:///---path to be modified---HealthcareETL/new.csv");
		//paymentsDs.write().csv("hdfs:///---path to be modifoed----");
		System.out.println("***********File write successful*********\n\n");
		//conversion to JSON format
		System.out.println("JSON format data:");
		paymentsDs.write().json("file:///---path to be modified---HealthcareETL/new.json");
		paymentsDs.toJSON().show();
		
		System.out.println("........End Of Program.......");		
	}
}
