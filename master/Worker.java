import java.util.ArrayList;

public class Worker {

	private int id;
	ArrayList<Job> joblist;
	private int status;
	private int processnum;
	private int index;
	private String host;
	private int port;
	
	public Worker(int id, String host, int port) {
		processnum = -1;
		this.id = id;
		joblist = new ArrayList<Job>();
		this.host = host;
		this.port = port;
	}
	
	public int getId() {
		return id;
	}
	
	public synchronized void putJob(Job job) {
	    joblist.add(job);
	}
	
	public synchronized boolean existNewJob() {
		if (joblist.size() > index) {
			return true;
		} else {
			return false;
		}
	}
	
	public synchronized Job getNewJob() {
		Job job = joblist.get(index);
		index++;
		return job;
	}
	
	public synchronized Job getJob(int index) {
		return joblist.get(index);
	}
	
	public synchronized int getListLength() {
		return joblist.size();
	}
	
	public synchronized void setStatus(int status) {
		this.status = status;
	}
	
	public synchronized String getStatus() {
		switch(status) {
		case 0: 
			return "running";
		case 1:
			return "down";
		default:
			return "connecting";
		}
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setProcessNum(int processnum) {
		this.processnum = processnum;
	}
	
	public int getProcessNum() {
		return processnum;
	}	
}
