package de.huberlin.wbi.dcs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;

// currently assumes no data transfer times (similar to CloudSim)
// and is provided with runtime estimates per VM as if there were only one taskslot per Vm
//vm有一个slot很方便，相当于task时分复用vm，可以直接使用
public class HCOC3 extends HybridDCBroker {
	
	public class TaskUpwardRankComparator implements Comparator<Task> {
		
		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(upwardRanks.get(task2), upwardRanks.get(task1));
		}

	}
	public class EFTComparator implements Comparator<Task> {
		
		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(EFTlist.get(task2), EFTlist.get(task1));
		}

	}
	private Map<Vm, TreeSet<Double>> freeTimeSlotStartsPerVm;//vm的空闲slot的起始时间点，为什么是一个二叉树，可能只是为了排序方便？
	private Map<Vm, Map<Double, Double>> freeTimeSlotLengthsPerVm;//vm的空闲slot时长<vm，<slot起点，slot时长>>
	private Map<Task, Double> upwardRanks;
	private Map<Task, Double> EFTlist;
	private ArrayList<Task> publist;
	private Map<Task,Vm> prilist;
	public HCOC3(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		upwardRanks = new HashMap<>();
		EFTlist=new HashMap<>();
		publist=new ArrayList<>();
		prilist=new HashMap<>();
		freeTimeSlotStartsPerVm = new HashMap<>();
		freeTimeSlotLengthsPerVm = new HashMap<>();
		
	}
	
	@Override
	public void doschedule(Collection<Task> tasks) {
		for (Vm vm : hybridVMlist.get("private")) {
			if (!readyTasks.containsKey(vm)) {
				Queue<Task> q = new LinkedList<>();
				readyTasks.put(vm, q);
			}
		}
		for (Vm vm : hybridVMlist.get("public")) {
			if (!readyTasks.containsKey(vm)) {
				Queue<Task> q = new LinkedList<>();
				readyTasks.put(vm, q);
			}
		}
		List<Task> sortedTasks = ranktask(tasks);
	
		// Phase 2: Processor Selection		
		double dl=sortedTasks.get(0).getWorkflow().getDeadline();
		tempschedule(sortedTasks,dl);
		Collections.sort(sortedTasks,new EFTComparator());
		Task pubTask=sortedTasks.get(0);
		double makespan=EFTlist.get(pubTask);
		while(makespan>dl&&publist.size()<sortedTasks.size()){
Log.printLine("makespan: "+makespan+" task:"+pubTask.getCloudletId());
			//prilist.remove(pubTask);
			publist.add(pubTask);
			//EFTlist.put(pubTask, bestFinishPublic(pubTask));
			
			tempschedule(sortedTasks,dl);
			Collections.sort(sortedTasks,new EFTComparator());
			pubTask=sortedTasks.get(0);
		}
			
		finalschedule();
	}
	// compute upward ranks of all tasks
	public List<Task> ranktask(Collection<Task> tasks){
		List<Task>sortedTasks=new ArrayList<>(tasks);
		Collections.sort(sortedTasks);
			for (int i = sortedTasks.size() - 1; i >= 0; i--) {
				Task task = sortedTasks.get(i);
				double maxSuccessorRank = 0;
				
				for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
					Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
					if (upwardRanks.get(child) > maxSuccessorRank) {
						maxSuccessorRank = upwardRanks.get(child);
					}
				}

				double avgcost = 0;
				Vm vm=hybridVMlist.get("private").get(0);
				avgcost= (task.getCloudletLength()/(vm.getMips()*vm.getNumberOfPes())
							+task.getCloudletOutputSize())/vm.getBw();			
				// note that the upward rank of a task will always be greater than that of its successors
				upwardRanks.put(task, avgcost + maxSuccessorRank);
			}
			
			// Phase 1: Task Prioritizing (sort by decreasing order of rank)
			Collections.sort(sortedTasks, new TaskUpwardRankComparator());
			return sortedTasks;
	}
	
	public void tempschedule(List<Task> sortedTasks, double dl){
		cleartemp(sortedTasks);
		
		for (Task task : sortedTasks) {			
			
			double readyTime = 0.0;
			for (DataDependency inEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(inEdge);
				
				double tempReady=EFTlist.get(parent);
Log.printLine("task: "+task.getCloudletId()+"--- parent: "+parent.getCloudletId()+" EFT: "+tempReady);	
				if(tempPrivate(parent)&&tempPrivate(task))
					tempReady+=parent.getCloudletOutputSize()/prilist.get(parent).getBw();
				else{
					PrivateDatacenter hdc=(PrivateDatacenter)hybridVMlist.get("public").get(0).getHost().getDatacenter();
					tempReady+=parent.getCloudletOutputSize()/hdc.getOutBw();
				}
				if(readyTime<tempReady)
					readyTime=tempReady;
			}

			Vm bestVm = null;
			double bestVmFreeTimeSlotActualStart = Double.MAX_VALUE;
			double bestFinish = Double.MAX_VALUE;//记录一个task在所有vm上，能最早完成的时间，把task调度在那个vm上
			double freeTimeSlotActualStart=Double.MAX_VALUE;
			if(publist.contains(task)){
				bestFinish=readyTime+task.getCloudletLength()/hybridVMlist.get("public").get(0).getCloudletScheduler().getCurrentAvailableRate();
				task.setSubmitTime(CloudSim.clock());
				task.setScheduleTime(CloudSim.clock());
	//Log.printLine("task:"+task.getCloudletId()+" EFT: "+bestFinish+"--");
					EFTlist.put(task, bestFinish);
			}
				
			else{
			for (HybridVm vm : hybridVMlist.get("private")) {
				double computationCost= task.getCloudletLength()/vm.getCloudletScheduler().getCurrentAvailableRate();
			
			TreeSet<Double> freeTimeSlotStarts = vm.getFreeTimeSlotStart();
			Map<Double, Double> freeTimeSlotLengths = vm.getFreeTimeSlotLength();
			//找该vm里，在ready time之后第一个空闲slot的开始时间
				SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts.floor(readyTime) != null) 
					? freeTimeSlotStarts.tailSet(freeTimeSlotStarts.floor(readyTime))
					: freeTimeSlotStarts.tailSet(freeTimeSlotStarts.ceiling(readyTime));
					
			//为task找该vm中，最近一块空闲slot调度
				for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {
					double freeTimeSlotLength = freeTimeSlotLengths.get(freeTimeSlotStart);
					freeTimeSlotActualStart = Math.max(readyTime, freeTimeSlotStart);				
					if (freeTimeSlotActualStart + computationCost > bestFinish)
						break;
					if (freeTimeSlotActualStart > freeTimeSlotStart)
						freeTimeSlotLength -= freeTimeSlotActualStart- freeTimeSlotStart;
					if (computationCost < freeTimeSlotLength) {
						bestVm = vm;
						bestVmFreeTimeSlotActualStart = freeTimeSlotActualStart;
						bestFinish = freeTimeSlotActualStart + computationCost;
//Log.printLine(task.getCloudletId()+"-计算开销："+computationCost+"-slot:-"+freeTimeSlotActualStart);
					}
				}
			}
			
			task.setSubmitTime(CloudSim.clock());
			task.setScheduleTime(CloudSim.clock());
//Log.printLine("task:"+task.getCloudletId()+" EFT: "+bestFinish+"--");
			
		/*	if(bestFinish>dl){
				publist.add(task);
				bestFinish=bestFinishPublic(task);
				EFTlist.put(task, bestFinish);
			}
			else{*/
				//prilist.put(task, bestVm);
				EFTlist.put(task, bestFinish);
				//标记private vm
				double timeslotStart = freeTimeSlotStartsPerVm.get(bestVm).floor(bestVmFreeTimeSlotActualStart);
				double timeslotLength = freeTimeSlotLengthsPerVm.get(bestVm).get(timeslotStart);
				double diff = bestVmFreeTimeSlotActualStart - timeslotStart;
				// add time slots before and after
				//如果是“slot开始~task执行~slot结束”的情况，要把后半部分去掉
				if (bestVmFreeTimeSlotActualStart > timeslotStart) 
					freeTimeSlotLengthsPerVm.get(bestVm).put(timeslotStart, diff);
				//如果是“task执行=slot开始~slot结束”的情况，要把这一段“slot开始=task执行~slot结束”slot去掉
				else {
					freeTimeSlotStartsPerVm.get(bestVm).remove(timeslotStart);
					freeTimeSlotLengthsPerVm.get(bestVm).remove(timeslotStart);
				}
				//因为从上面的循环里面出来了，所以task的计算开销没有了，就用已知的参数再算一遍
				double computationCost = bestFinish - bestVmFreeTimeSlotActualStart;
				//把空闲slot里，减掉task占掉的部分，后面的部分加在空闲slot列表里面
				double actualTimeSlotLength = timeslotLength - diff;
				if (computationCost < actualTimeSlotLength) {
					freeTimeSlotStartsPerVm.get(bestVm).add(bestFinish);
					freeTimeSlotLengthsPerVm.get(bestVm).put(bestFinish,actualTimeSlotLength - computationCost);
				}
			}
		}
	}
	
	
	public double bestFinishPublic(Task t){
		double readyTime=0.0;
		for (DataDependency inEdge : t.getWorkflow().getGraph().getInEdges(t)) {
			Task parent = t.getWorkflow().getGraph().getSource(inEdge);
			double tempReady=EFTlist.get(parent);

			if(tempPrivate(parent)){
				PrivateDatacenter hdc=(PrivateDatacenter)hybridVMlist.get("public").get(0).getHost().getDatacenter();
				tempReady+=parent.getCloudletOutputSize()/hdc.getOutBw();
			}
			else	
				tempReady+=parent.getCloudletOutputSize()/schedule.get(parent).getBw();
			if(readyTime<tempReady)
				readyTime=tempReady;
		}
		double computecost=t.getCloudletLength()/hybridVMlist.get("public").get(0).getCloudletScheduler().getCurrentAvailableRate();
//Log.printLine("task: "+t.getCloudletId()+" EST: "+readyTime+"-compute-"+computecost+" mips: "+hybridVMlist.get("public").get(0).getCloudletScheduler().getCurrentAvailableRate());		
		
		return readyTime+computecost;
		}
	
	public void finalschedule(){
		schedule.putAll(prilist);
		for(HybridVm vm:hybridVMlist.get("private")){
			vm.getFreeTimeSlotStart().addAll(freeTimeSlotStartsPerVm.get(vm));
			vm.getFreeTimeSlotLength().putAll(freeTimeSlotLengthsPerVm.get(vm));
		}
		
		for(Task task:publist){
			for(Vm vm:hybridVMlist.get("public")){
				if(vm.getNumberOfPes()>task.getNumberOfPes())
				{
					schedule.put(task, vm);
					break;
				}
			}
		}
	}
	public boolean tempPrivate(Task task){
		if(prilist.containsKey(task))
			return true;
		return false;
	}
	public void cleartemp(List<Task> tasks){
		prilist.clear();
		freeTimeSlotStartsPerVm.clear();
		freeTimeSlotLengthsPerVm.clear();
		for(HybridVm vm:hybridVMlist.get("private")){
			freeTimeSlotStartsPerVm.put(vm, vm.getFreeTimeSlotStart());
			freeTimeSlotLengthsPerVm.put(vm, vm.getFreeTimeSlotLength());
		}
		EFTlist.clear();
		for(Task t:tasks)
			EFTlist.put(t, 0d);
	}
}
