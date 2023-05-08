package ETL.Healtcare;

//payments class for custom schema
public class Payments {
	private static final long serialVersionUID = 1L;
	
	// Code standardization WIP
	private String physicianId,dateOfPayment,recordId,payer,physicianSpeciality,natureOfPayment;
	private double amount;
	public String getPhysicianId() {
		return physicianId;
	}
	public void setPhysicianId(String physicianId) {
		this.physicianId = physicianId;
	}
	public String getDateOfPayment() {
		return dateOfPayment;
	}
	public void setDateOfPayment(String dateOfPayment) {
		this.dateOfPayment = dateOfPayment;
	}
	public String getRecordId() {
		return recordId;
	}
	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}
	public String getPayer() {
		return payer;
	}
	public void setPayer(String payer) {
		this.payer = payer;
	}
	public String getPhysicianSpeciality() {
		return physicianSpeciality;
	}
	public void setPhysicianSpeciality(String physicianSpeciality) {
		this.physicianSpeciality = physicianSpeciality;
	}
	public String getNatureOfPayment() {
		return natureOfPayment;
	}
	public void setNatureOfPayment(String natureOfPayment) {
		this.natureOfPayment = natureOfPayment;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	
}
