package hu.lamsoft.db.meter;

import java.sql.SQLException;
import java.util.Date;

import hu.lamsoft.db.meter.bulkinserter.BulkInserter;
import hu.lamsoft.db.meter.entity.TestInnoDBObject;

public class BulkInserterTest extends Thread {

	BulkInserter<TestInnoDBObject> bulkInserter;
	int count;
	
	public BulkInserterTest(BulkInserter<TestInnoDBObject> bulkInserter, int count) {
		super();
		this.bulkInserter = bulkInserter;
		this.count = count;
	}

	@Override
	public void run() {
		super.run();
		for(int i = 0; i<count; i++) {
			try {
				bulkInserter.add(generateTestInnoDBObject());
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
	}
	
    private TestInnoDBObject generateTestInnoDBObject() {
    	TestInnoDBObject ret = new TestInnoDBObject();
    	
    	ret.setName("alma");
    	ret.setCount(100);
    	ret.setPrice(10.54);
    	ret.setText("dsaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaasdfdfhsdfghsfgbcxvhrtsyrnsrznsznsrnzszfgjhdfkljhdfgkhfdlkjhdsflgkhslkhskjdlksdfhljhgfdsnbeátijabűátjbésáprjűénjokrsézpjkpfxogkŰŐÁ NKFAŐ");
    	ret.setCreateDate(new Date());
    	ret.setUpdateDate(new Date());
    	
    	return ret;
    }
}
