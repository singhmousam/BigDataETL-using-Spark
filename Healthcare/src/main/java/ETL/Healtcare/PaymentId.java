package ETL.Healtcare;

public class PaymentId {
	private static final long serialVersionUID = 1L;

	private String _id,physicianId,dateOfPayment,recordId,payer,physicianSpeciality,natureOfPayment;
	private double amount;
	
	public PaymentId(String _id, String physicianId, String dateOfPayment, String payer, double amount,
			String physicianSpeciality, String natureOfPayment) {
		super();
		this._id = _id;
		this.physicianId = physicianId;
		this.dateOfPayment = dateOfPayment;
		this.recordId = recordId;
		this.payer = payer;
		this.physicianSpeciality = physicianSpeciality;
		this.natureOfPayment = natureOfPayment;
		this.amount = amount;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
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