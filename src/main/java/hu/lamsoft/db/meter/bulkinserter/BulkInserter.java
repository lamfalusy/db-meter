package hu.lamsoft.db.meter.bulkinserter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkInserter<T> {
	
	private static final Logger log = LoggerFactory.getLogger(BulkInserter.class);
	
	private int bulkSize = 500;
	private ConcurrentLinkedQueue<T> queue;
	private AtomicInteger queueSize;
	private AtomicInteger queuePublicSize;
	private Connection connection;
	private Lock lock;
	private Function<T, String> objectToInsertSqlValueFunction;
	private String tableName;
	private String columnInsertSql;
	
	public BulkInserter(int bulkSize, Connection connection, Function<T, String> objectToInsertSqlValueFunction, String tableName, String columnInsertSql) {
		this(connection, objectToInsertSqlValueFunction, tableName, columnInsertSql);
		this.bulkSize = bulkSize;
	}
	
	public BulkInserter(Connection connection, Function<T, String> objectToInsertSqlValueFunction, String tableName, String columnInsertSql) {
		this();
		this.connection = connection;
		this.objectToInsertSqlValueFunction = objectToInsertSqlValueFunction;
		this.tableName = tableName;
		this.columnInsertSql = columnInsertSql;
	}

	private BulkInserter() {
		super();
		this.queue = new ConcurrentLinkedQueue<>();
		this.queueSize = new AtomicInteger();
		this.queuePublicSize = new AtomicInteger();
		this.lock = new ReentrantLock();
	}
	
	public void flush() throws SQLException {
		bulkInsert(queueSize.get());
	}
	
	public void add(T t) throws SQLException {
		queueSize.incrementAndGet();
		queuePublicSize.incrementAndGet();
		queue.add(t);
		
		boolean isNecessaryToInsert = false;
		
		if(lock.tryLock()) {
			isNecessaryToInsert = queuePublicSize.get() > bulkSize;
			if(isNecessaryToInsert) {
				queuePublicSize.getAndAdd(-bulkSize);
			}
			lock.unlock();
		}
		
		if(isNecessaryToInsert) {
			bulkInsert();
		}
	}
	
	private void bulkInsert() throws SQLException {
		log.info(Thread.currentThread().getId()+" will process insert "+" "+queueSize+" "+queuePublicSize);
		bulkInsert(this.bulkSize);
	}
	
	private void bulkInsert(int bulkSize) throws SQLException {
		Statement statement = connection.createStatement();
		StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName).append(" ").append(columnInsertSql).append(" VALUES ");
		
		for(int i = 0; i<bulkSize; i++) {
			queueSize.decrementAndGet();
			T t = queue.remove();
			sb.append(objectToInsertSqlValueFunction.apply(t));
			if(i!=bulkSize-1) {
				sb.append(",");
			}
		}
		
		statement.executeUpdate(sb.toString());
		statement.close();
	}
	
	public void close() throws SQLException {
		connection.close();
	}
	
}
