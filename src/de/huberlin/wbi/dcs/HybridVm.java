package de.huberlin.wbi.dcs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.examples.Parameters;

public class HybridVm extends Vm {
	
	private long io;
	
	private double numberOfCusPerPe;
	
	private DynamicModel dynamicModel;
	
	private double currentMiCoefficient;
	private double currentIoCoefficient;
	private double currentBwCoefficient;
	
	private double previousTime;
	
	private boolean isPrivate;
	private double price;
	private BufferedWriter performanceLog;
	private TreeSet<Double> freeTimeSlotStart;//vm的空闲slot的起始时间点，为什么是一个二叉树，可能只是为了排序方便？
	private Map<Double, Double> freeTimeSlotLength;//vm的空闲slot时长<vm，<slot起点，slot时长>>

	
	public HybridVm(
			int id,
			int userId,
			double numberOfCusPerPe,
			int numberOfPes,
			int ram,
			long storage,
			String vmm,
			CloudletScheduler cloudletScheduler,
			DynamicModel dynamicModel,
			String performanceLogFileName,
			int taskSlots,
			boolean isPrivate,
			Double price) {
		super(id, userId, -1, numberOfPes, ram, -1, storage, vmm, cloudletScheduler);
		setNumberOfCusPerPe(numberOfCusPerPe);
		setDynamicModel(dynamicModel);
		setCoefficients();
		previousTime = CloudSim.clock();

		this.isPrivate=isPrivate;
		setPrice(price);
		freeTimeSlotStart=new TreeSet<>();
		freeTimeSlotStart.add(0d);
		freeTimeSlotLength=new HashMap<>();
		freeTimeSlotLength.put(0d, Double.MAX_VALUE);
	}
	
	public HybridVm(
			int id,
			int userId,
			double numberOfCusPerPe,
			int numberOfPes,
			int ram,
			long storage,
			String vmm,
			CloudletScheduler cloudletScheduler,
			DynamicModel dynamicModel,
			String performanceLogFileName) {
		this(id, userId, numberOfCusPerPe, numberOfPes, ram, storage, vmm, cloudletScheduler, dynamicModel, performanceLogFileName, 1,true, 0.0);
	}
	
	public void updatePerformanceCoefficients () {
		double currentTime = CloudSim.clock();
		double timespan = currentTime - getPreviousTime();
		setPreviousTime(currentTime);
		dynamicModel.updateBaselines(timespan);
		setCoefficients();
	}
	
	private void setCoefficients() {
		setCurrentMiCoefficient(dynamicModel.nextMiCoefficient());
		setCurrentIoCoefficient(dynamicModel.nextIoCoefficient());
		setCurrentBwCoefficient(dynamicModel.nextBwCoefficient());
	}
	
	public void setMips(double mips) {
		super.setMips(mips);
	}
	
	public long getIo() {
		return io;
	}
	
	public double getNumberOfCusPerPe() {
		return numberOfCusPerPe;
	}
	
	public void setIo(long io) {
		this.io = io;
	}
	
	public void setNumberOfCusPerPe(double numberOfCusPerPe) {
		this.numberOfCusPerPe = numberOfCusPerPe;
	}
	
	public DynamicModel getDynamicModel() {
		return dynamicModel;
	}
	
	public void setDynamicModel(DynamicModel dynamicModel) {
		this.dynamicModel = dynamicModel;
	}
	
	public double getCurrentBwCoefficient() {
		return currentBwCoefficient;
	}
	
	public double getCurrentIoCoefficient() {
		return currentIoCoefficient;
	}
	
	public double getCurrentMiCoefficient() {
		return currentMiCoefficient;
	}
	
	private void setCurrentBwCoefficient(double currentBwCoefficient) {
		this.currentBwCoefficient = currentBwCoefficient;
	}
	
	private void setCurrentIoCoefficient(double currentIoCoefficient) {
		this.currentIoCoefficient = currentIoCoefficient;
	}
	
	private void setCurrentMiCoefficient(double currentMiCoefficient) {
		this.currentMiCoefficient = currentMiCoefficient;
	}
	
	public double getPreviousTime() {
		return previousTime;
	}
	
	public void setPreviousTime(double previousTime) {
		this.previousTime = previousTime;
	}
	
	public BufferedWriter getPerformanceLog() {
		return performanceLog;
	}
	
	public void closePerformanceLog() {
		if (Parameters.outputVmPerformanceLogs) {
			try {
				performanceLog.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	


	public boolean isPrivate() {
		return isPrivate;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public TreeSet<Double> getFreeTimeSlotStart() {
		return freeTimeSlotStart;
	}

	public void setFreeTimeSlotStart(TreeSet<Double> freeTimeSlotStart) {
		this.freeTimeSlotStart = freeTimeSlotStart;
	}

	public Map<Double, Double> getFreeTimeSlotLength() {
		return freeTimeSlotLength;
	}

	public void setFreeTimeSlotLength(Map<Double, Double> freeTimeSlotLength) {
		this.freeTimeSlotLength = freeTimeSlotLength;
	}
}
