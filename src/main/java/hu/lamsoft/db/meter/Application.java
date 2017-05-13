package hu.lamsoft.db.meter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StopWatch;

import hu.lamsoft.db.meter.bulkinserter.BulkInserter;
import hu.lamsoft.db.meter.entity.TestInnoDBObject;
import hu.lamsoft.db.meter.entity.TestMyISAMObject;
import hu.lamsoft.db.meter.repository.TestInnoDBObjectRepository;
import hu.lamsoft.db.meter.repository.TestMyISAMObjectRepository;

@SpringBootApplication
public class Application {

	@Autowired
	DataSource dataSource;
	
	@Autowired
	ThreadPoolTaskExecutor taskExecutor;
	
	private static final Logger log = LoggerFactory.getLogger(Application.class);
	
    public static void main(String[] args) {
        SpringApplication.run(Application.class);
    }

    @Bean
    public CommandLineRunner demo(TestInnoDBObjectRepository testInnoDBObjectRepository, TestMyISAMObjectRepository testMyISAMObjectRepository) {
        return (args) -> {
        	
//        	log.info("InnoDB started");
//        	doInnoDB(testInnoDBObjectRepository, 1000, 100);
//        	doInnoDB(testInnoDBObjectRepository, 400, 250);
//        	doInnoDB(testInnoDBObjectRepository, 200, 500);
//        	doInnoDB(testInnoDBObjectRepository, 100, 1000);
//            
//        	log.info("MyISAM started");
//        	doMyISAM(testMyISAMObjectRepository, 1000, 100);
//        	doMyISAM(testMyISAMObjectRepository, 400, 250);
//        	doMyISAM(testMyISAMObjectRepository, 200, 500);
//        	doMyISAM(testMyISAMObjectRepository, 100, 1000);
//        	
//        	Connection connection = dataSource.getConnection();
//        	log.info("InnoDB JDBC started");
//        	doInnoDBJDBC(connection, 1000, 100);
//        	doInnoDBJDBC(connection, 400, 250);
//        	doInnoDBJDBC(connection, 200, 500);
//        	doInnoDBJDBC(connection, 100, 1000);
//        	
//        	log.info("MyISAM JDBC started");
//        	doMyISAMJDBC(connection, 1000, 100);
//        	doMyISAMJDBC(connection, 400, 250);
//        	doMyISAMJDBC(connection, 200, 500);
//        	doMyISAMJDBC(connection, 100, 1000);
//        	connection.close();
        	
//        	log.info("InnoDB BulkInserter started");
//        	doInnoDBBulkInsert(1000, 100);
//        	doInnoDBBulkInsert(400, 250);
//        	doInnoDBBulkInsert(200, 500);
//        	doInnoDBBulkInsert(100, 1000);
//        	
//        	log.info("MyISAM BulkInserter started");
//        	doMyISAMBulkInsert(1000, 100);
//        	doMyISAMBulkInsert(400, 250);
//        	doMyISAMBulkInsert(200, 500);
//        	doMyISAMBulkInsert(100, 1000);
        	
        	log.info("InnoDB BulkInserter started");
        	doInnoDBBulkInsertMultiThread(200, 1000, 1);
        	doInnoDBBulkInsertMultiThread(200, 1000, 10);
        	doInnoDBBulkInsertMultiThread(200, 1000, 100);
        };
    }

	private void doInnoDBBulkInsert(int bulkConut, int bulkSize) throws SQLException {
		StopWatch sw = new StopWatch("InnoDB bulkInserter");
    	sw.start("InnoDB bulkInserter "+bulkConut+" "+bulkSize);
    	BulkInserter<TestInnoDBObject> innoDBBulkInserter = new BulkInserter<>(bulkSize,dataSource.getConnection(), this::getInnoDBInsertValues, "test_innodbobject", "(count, name, price, text)");
    	for(int i = 0; i<bulkConut*bulkSize; i++) {
    		innoDBBulkInserter.add(generateTestInnoDBObject());
    	}
    	innoDBBulkInserter.close();
    	sw.stop();
        log.info(sw.prettyPrint());
    }
	
	private void doInnoDBBulkInsertMultiThread(int bulkConut, int bulkSize, int threads) throws SQLException, InterruptedException, ExecutionException {
		StopWatch sw = new StopWatch("InnoDB bulkInserter multi thread");
    	sw.start("InnoDB bulkInserter multi thread "+bulkConut+" "+bulkSize+" "+threads);
    	BulkInserter<TestInnoDBObject> innoDBBulkInserter = new BulkInserter<>(bulkSize,dataSource.getConnection(), this::getInnoDBInsertValues, "test_innodbobject", "(count, name, price, text)");
    	
    	Collection<Future<?>> futures = new LinkedList<Future<?>>();
    	int countPerThread = (bulkConut*bulkSize)/threads;
    	for(int i = 0; i<threads; i++) {
    		futures.add(taskExecutor.submit(new BulkInserterTest(innoDBBulkInserter, countPerThread)));
    	}
    	for (Future<?> future:futures) {
    	    future.get();
    	}
    	innoDBBulkInserter.flush();
    	innoDBBulkInserter.close();
    	sw.stop();
        log.info(sw.prettyPrint());
    }

    private void doMyISAMBulkInsert(int bulkConut, int bulkSize) throws SQLException {
    	StopWatch sw = new StopWatch("MyISAM bulkInserter");
    	sw.start("MyISAM bulkInserter "+bulkConut+" "+bulkSize);
    	BulkInserter<TestMyISAMObject> myISAMBulkInserter = new BulkInserter<>(bulkSize,dataSource.getConnection(), this::getMyISAMInsertValues, "test_myisamobject", "(count, name, price, text)");
    	for(int i = 0; i<bulkConut*bulkSize; i++) {
    		myISAMBulkInserter.add(generateTestMyISAMObject());
    	}
    	myISAMBulkInserter.close();
    	sw.stop();
        log.info(sw.prettyPrint());
	}
	
	public void doInnoDB(TestInnoDBObjectRepository testInnoDBObjectRepository, int count, int size) {
    	StopWatch sw = new StopWatch("InnoDB");
    	sw.start("InnoDB "+count+" "+size);
    	SynchronizedDescriptiveStatistics stat2 = new SynchronizedDescriptiveStatistics();
        
    	for(int i = 0; i< count; i++) {
    		ArrayList<TestInnoDBObject> list = new ArrayList<>(size);
    		StopWatch sw2 = new StopWatch("save rows");
    		for(int j = 0; j< size; j++) {
    			list.add(generateTestInnoDBObject());
    		}
    		sw2.start();
    		testInnoDBObjectRepository.save(list);
    		sw2.stop();
    		stat2.addValue(sw2.getTotalTimeMillis());
    	}
    	
        sw.stop();
        log.info(sw.prettyPrint());
        log.info("InnoDB "+count+" "+size+" mean: {}, min: {}, max: {}", stat2.getMean(), stat2.getMin(), stat2.getMax());
    }
    
    public void doMyISAM(TestMyISAMObjectRepository testMyISAMObjectRepository, int count, int size) {
    	StopWatch sw = new StopWatch("MyISAM");
    	sw.start("MyISAM "+count+" "+size);
    	SynchronizedDescriptiveStatistics stat2 = new SynchronizedDescriptiveStatistics();
        
    	for(int i = 0; i< count; i++) {
    		ArrayList<TestMyISAMObject> list = new ArrayList<>(size);
    		StopWatch sw2 = new StopWatch("save rows");
    		for(int j = 0; j< size; j++) {
    			list.add(generateTestMyISAMObject());
    		}
    		sw2.start();
    		testMyISAMObjectRepository.save(list);
    		sw2.stop();
    		stat2.addValue(sw2.getTotalTimeMillis());
    	}
    	
        sw.stop();
        log.info(sw.prettyPrint());
        log.info("MyISAM "+count+" "+size+" mean: {}, min: {}, max: {}", stat2.getMean(), stat2.getMin(), stat2.getMax());
    }
    
    public void doInnoDBJDBC(Connection connection, int count, int size) throws SQLException {
    	StopWatch sw = new StopWatch("InnoDB JDBC");
    	sw.start("InnoDB JDBC "+count+" "+size);
    	SynchronizedDescriptiveStatistics stat2 = new SynchronizedDescriptiveStatistics();
        
    	for(int i = 0; i< count; i++) {
    		StopWatch sw2 = new StopWatch("save rows");
    		Statement stmt = connection.createStatement();
    		StringBuilder sb = new StringBuilder("INSERT INTO test_innodbobject (count, name, price, text) VALUES ");
    		
    		for(int j = 0; j< size; j++) {
    			TestInnoDBObject t = generateTestInnoDBObject();
    			sb.append(getInnoDBInsertValues(t));
    			if(j!=size-1) {
    				sb.append(",");
    			}	
    		}
    		
    		sw2.start();
    		stmt.executeUpdate(sb.toString());
    		sw2.stop();
    		stat2.addValue(sw2.getTotalTimeMillis());
    		stmt.close();
    	}
    	
        sw.stop();
        log.info(sw.prettyPrint());
        log.info("InnoDB JDBC "+count+" "+size+" mean: {}, min: {}, max: {}", stat2.getMean(), stat2.getMin(), stat2.getMax());
    }
    
    public String getInnoDBInsertValues(TestInnoDBObject t) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("(");
		sb.append(t.getCount());
		sb.append(",'");
		sb.append(t.getName());
		sb.append("',");
		sb.append(t.getPrice());
		sb.append(",'");
		sb.append(t.getText());
		sb.append("')");
		return sb.toString();
    }
    
    public String getMyISAMInsertValues(TestMyISAMObject t) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("(");
		sb.append(t.getCount());
		sb.append(",'");
		sb.append(t.getName());
		sb.append("',");
		sb.append(t.getPrice());
		sb.append(",'");
		sb.append(t.getText());
		sb.append("')");
		return sb.toString();
    }
    
    public void doMyISAMJDBC(Connection connection, int count, int size) throws SQLException {
    	StopWatch sw = new StopWatch("MyISAM JDBC");
    	sw.start("MyISAM JDBC "+count+" "+size);
    	SynchronizedDescriptiveStatistics stat2 = new SynchronizedDescriptiveStatistics();
        
    	for(int i = 0; i< count; i++) {
    		StopWatch sw2 = new StopWatch("save rows");
    		Statement stmt = connection.createStatement();
    		StringBuilder sb = new StringBuilder("INSERT INTO test_myisamobject (count, name, price, text) VALUES ");
    		
    		for(int j = 0; j< size; j++) {
    			TestInnoDBObject t = generateTestInnoDBObject();
    			sb.append("(");
    			sb.append(t.getCount());
    			sb.append(",'");
    			sb.append(t.getName());
    			sb.append("',");
    			sb.append(t.getPrice());
    			sb.append(",'");
    			sb.append(t.getText());
    			sb.append("')");
    			if(j!=size-1) {
    				sb.append(",");
    			}	
    		}
    		
    		sw2.start();
    		stmt.executeUpdate(sb.toString());
    		sw2.stop();
    		stat2.addValue(sw2.getTotalTimeMillis());
    		stmt.close();
    	}
    	
        sw.stop();
        log.info(sw.prettyPrint());
        log.info("MyISAM JDBC "+count+" "+size+" mean: {}, min: {}, max: {}", stat2.getMean(), stat2.getMin(), stat2.getMax());
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
    
    private TestMyISAMObject generateTestMyISAMObject() {
    	TestMyISAMObject ret = new TestMyISAMObject();
    	
    	ret.setName("alma");
    	ret.setCount(100);
    	ret.setPrice(10.54);
    	ret.setText("dsaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaasdfdfhsdfghsfgbcxvhrtsyrnsrznsznsrnzszfgjhdfkljhdfgkhfdlkjhdsflgkhslkhskjdlksdfhljhgfdsnbeátijabűátjbésáprjűénjokrsézpjkpfxogkŰŐÁ NKFAŐ");
    	ret.setCreateDate(new Date());
    	ret.setUpdateDate(new Date());
    	
    	return ret;
    }
    
}
