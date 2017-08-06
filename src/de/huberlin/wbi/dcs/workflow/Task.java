package de.huberlin.wbi.dcs.workflow;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
public class Task extends Cloudlet implements Comparable<Task> {
	
	private final String name;
	
	private final String params;
	
	private int nDataDependencies;
	
	private Workflow workflow;
	private Partition partition;
	
	private int depth;
	
	private static int maxDepth;
	private List<File> inputFiles;
	private double lst=CloudSim.clock();
	private double est=CloudSim.clock();
	private Map<Task,File> inputFilelist;
	private Map<Task,File> outputFilelist;
	private boolean isPrivate;
	
	public Task(
			final String name,
			final String params,
			final Workflow workflow,
			final int userId,
			final int cloudletId,
			final long cloudletLength,
			final long ioLength,
			final long bwLength,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final UtilizationModel utilizationModelCpu,
			final UtilizationModel utilizationModelRam,
			final UtilizationModel utilizationModelBw) {
		super(
		cloudletId,
		cloudletLength,
		pesNumber,
		cloudletFileSize,
		cloudletOutputSize,
		utilizationModelCpu,
		utilizationModelRam,
		utilizationModelBw);

		this.name = name;
		this.params=params;
		this.workflow = workflow;
		this.setUserId(userId);
		this.depth = 0;
		inputFiles = new ArrayList<>();
		inputFilelist=new HashMap<>();
		outputFilelist=new HashMap<>();
		isPrivate=true;
	}
	
/*	public Task(Task task) {
		this(task.getName(),
				task.getParams(),
				task.getWorkflow(),
				task.getUserId(),
				task.getCloudletId(),
				task.getMi(),
				task.getIo(),
				task.getBw(),
				task.getNumberOfPes(),
				task.getCloudletFileSize(),
				task.getCloudletOutputSize(),
				task.getUtilizationModelCpu(),
				task.getUtilizationModelRam(),
				task.getUtilizationModelBw());
	}*/
	
	public String getName() {
		return name;
	}
	
	public String getParams() {
		return params;
	}
	
	public String toString() {
		return getName();
	}
	
	public void incNDataDependencies() {
		nDataDependencies++;
	}
	
	public void decNDataDependencies() {
		nDataDependencies--;
	}
	
	public boolean readyToExecute() {
		return nDataDependencies == 0;
	}
	public boolean readyToExecute(double time) {
		return nDataDependencies == 0 && time>=getSubmitTime();
	}
	public Workflow getWorkflow() {
		return workflow;
	}
	
	public static int getMaxDepth() {
		return maxDepth;
	}
	
	public int getDepth() {
		return depth;
	}
	
	public void setDepth(int depth) {
		this.depth = depth;
		if (depth > maxDepth) {
			maxDepth = depth;
		}
	}
	
	@Override
	public int compareTo(Task o) {
		return (this.getDepth() == o.getDepth()) ? Double.compare(this.getCloudletId(), o.getCloudletId()) : Double.compare(this.getDepth(), o.getDepth());
	}
	
	@Override
    public boolean equals(Object arg0) {
		return ((Task)arg0).getCloudletId() == getCloudletId();
	}
	
    public int hashCode() {
		return getCloudletId();
    }

	public double getLst() {
		return lst;
	}

	public void setLst(double lst) {
		this.lst = lst;
	}
	public int getNDataDependencies(){
		return nDataDependencies;
	}

	public Map<Task,File> getInputFilelist() {
		return inputFilelist;
	}

	public Map<Task,File> getOutputFilelist() {
		return outputFilelist;
	}
	
	public void addInputFile(Task source, File file){
		inputFilelist.put(source, file);
	}
	public void addOutputFile(Task dest, File file){
		outputFilelist.put(dest, file);
	}
	
	public long getCloudletOutputSize(){
		long size=0;
		Iterator outs=getOutputFilelist().values().iterator();
		while(outs.hasNext()){
			File f=(File)outs.next();
			size+=f.getSize();
		}
		return size;
	}

	public double getEst() {
		return est;
	}

	public void setEst(double est) {
		this.est = est;
	}

	public Partition getPartition() {
		return partition;
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
	}

	public boolean isPrivate() {
		return isPrivate;
	}

	public void setPrivate(boolean isPrivate) {
		this.isPrivate = isPrivate;
	}
	public List<File> getInputFiles() {
		return inputFiles;
	}
	
	public void addInputFile(File file) {
		inputFiles.add(file);
	}
}
