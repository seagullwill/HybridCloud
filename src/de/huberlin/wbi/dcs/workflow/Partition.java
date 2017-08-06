package de.huberlin.wbi.dcs.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;

import de.huberlin.wbi.dcs.examples.Parameters;

public class Partition{
	
	private List<Task> tasks;
	private double weight;
	public Partition(List<Task> tasks){
		this.tasks = tasks;
		for(Task task:tasks)
			task.setPartition(this);
		setWeight(0.0);

	}
	public long taskCut(Task task){
		long cut=0;
		if(containTask(task))
			return 0;
		for(Task t:getTasks()){
			if(t.getInputFilelist().get(task)!=null)
				cut+=t.getInputFilelist().get(task).getSize();
			if(t.getOutputFilelist().get(task)!=null)
				cut+=t.getOutputFilelist().get(task).getSize();
		}
		return cut;
	}
	public long outerCut(){
		long cut=0;
		for(Task t:getTasks()){
			for(Task f:t.getInputFilelist().keySet()){
				if(!containTask(f))
					cut+=t.getInputFilelist().get(f).getSize();
			}
			for(Task f:t.getOutputFilelist().keySet()){
				if(!containTask(f))
					cut+=t.getOutputFilelist().get(f).getSize();
			}
		}
		return cut;
	}
	public long innerCut(){
		long cut=0;
		for(Task t:getTasks()){
			for(Task f:t.getInputFilelist().keySet()){
				if(containTask(f))
					cut+=t.getInputFilelist().get(f).getSize();
			}
			for(Task f:t.getOutputFilelist().keySet()){
				if(containTask(f))
					cut+=t.getOutputFilelist().get(f).getSize();
			}
		}
		return cut;
	}
	public void addTask(Task task){
		tasks.add(task);
		task.setPartition(this);
	}
	public void removeTask(Task task){
		tasks.remove(task);
	}
	public boolean containTask(Task task){
		if(getTasks().contains(task))
			return true;
		return false;
	}
	
	public void setWeight(double compute, double bw){
		double weight=0.0;
		Map<Task,Double> EFT=new HashMap();
		
		for (Task task:getSortedTasks()) {
			double maxest = 0;
			for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
				
				double transferTime=0;
				if(containTask(parent))
					transferTime+=task.getInputFilelist().get(parent).getSize()/bw;
				else{
					EFT.put(parent, 0.0);
					transferTime+=task.getInputFilelist().get(parent).getSize()/Parameters.OUTBW;
				}
//Log.printLine(task.getCloudletId()+"--"+parent.getCloudletId()+"---"+EFT.get(parent)+"---"+transferTime);				
				if (maxest<EFT.get(parent)+transferTime) 
					maxest = EFT.get(parent)+transferTime;
				}
			
			EFT.put(task, maxest+task.getCloudletLength()/compute);
			double outTime=0.0;
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph().getSource(outgoingEdge);
				if(!containTask(child))
					outTime+=task.getOutputFilelist().get(child).getSize()/Parameters.OUTBW;
			}
			if(weight<maxest+task.getCloudletLength()/compute+outTime)
				weight=maxest+task.getCloudletLength()/compute+outTime;
		}	
		this.weight=weight;
	}
	
	public List<Task> getSortedTasks() {
		List<Task>sortedTasks=new ArrayList<>(tasks);
		Collections.sort(sortedTasks);
		return sortedTasks;
	}
	public List<Task> getTasks() {
		return tasks;
	}
	public double getWeight(double compute, double bw) {
		double w=0.0;
	
		for (Task t:getSortedTasks()) {
			w+=t.getCloudletLength()/compute;
			for (DataDependency outgoingEdge : t.getWorkflow().getGraph().getOutEdges(t)) {
				Task child = t.getWorkflow().getGraph().getDest(outgoingEdge);
				if(containTask(child))
					w+=t.getOutputFilelist().get(child).getSize()/bw;
			}
		}
		return w;
	}
	public long getLength(){
		long weight=0;
		Map<Task,Long> EFT=new HashMap();
		
		for (Task task:getSortedTasks()) {
			long maxest = 0;
			for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
				if(containTask(parent)){
					if (maxest<EFT.get(parent)) 
						maxest = EFT.get(parent);
					}
				}
			
			EFT.put(task, maxest+(long)task.getCloudletLength());
			
			if(weight<maxest+task.getCloudletLength())
				weight=maxest+task.getCloudletLength();
		}	
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}

	public boolean before(Task task){
		if(task.getPartition()==null){
			for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
				if(containTask(parent))
					return true;
			}
		}
		else{
			for(Task t:task.getPartition().getSortedTasks()){
				for (DataDependency ingoingEdge : t.getWorkflow().getGraph().getInEdges(t)) {
					Task parent = t.getWorkflow().getGraph().getSource(ingoingEdge);
					if(containTask(parent))
						return true;
					}
			}
		}
		return false;
	}
}
