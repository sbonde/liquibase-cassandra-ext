package liquibase.changelog;

import java.util.Date;

import liquibase.change.CheckSum;

public class CustomRanChangeSet extends RanChangeSet{

	private int orderExecuted;
	public CustomRanChangeSet(ChangeSet changeSet) {
		super(changeSet);
		// TODO Auto-generated constructor stub
	}
	public CustomRanChangeSet(ChangeSet changeSet, ChangeSet.ExecType execType, int orderExecuted) {
		super(changeSet.getFilePath(),
	             changeSet.getId(),
	             changeSet.getAuthor(),
	             changeSet.generateCheckSum(),
	             new Date(),
	             null,
	             execType,
	            changeSet.getDescription(),
	            changeSet.getComments());
		this.orderExecuted = orderExecuted;
	    }

	    public CustomRanChangeSet(String changeLog, String id, String author, CheckSum lastCheckSum, Date dateExecuted, String tag, ChangeSet.ExecType execType, String description, String comments, int orderExecuted) {
	    	super(changeLog, id, author, lastCheckSum, dateExecuted, tag,  execType, description, comments);
	    	this.orderExecuted = orderExecuted;
	    }

	public int getOrderExecuted() {
		return orderExecuted;
	}
	public void setOrderExecuted(int orderExecuted) {
		this.orderExecuted = orderExecuted;
	}

}
