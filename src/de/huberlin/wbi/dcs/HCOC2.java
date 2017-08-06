package de.huberlin.wbi.dcs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
public class HCOC2 extends HybridDCBroker {
	
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
	Map<Task, Double> readyTimePerTask;
	Map<Vm, TreeSet<Double>> freeTimeSlotStartsPerVm;//vm的空闲slot的起始时间点，为什么是一个二叉树，可能只是为了排序方便？
	Map<Vm, Map<Double, Double>> freeTimeSlotLengthsPerVm;//vm的空闲slot时长<vm，<slot起点，slot时长>>

	Map<Task, Double> upwardRanks;

	Map<Task, Map<Vm,Double>> taskVmSlot;
	Map<Task, Double> EFTlist;
	public HCOC2(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		readyTimePerTask = new HashMap<>();
		freeTimeSlotStartsPerVm = new HashMap<>();
		freeTimeSlotLengthsPerVm = new HashMap<>();
		upwardRanks = new HashMap<>();
		taskVmSlot=new HashMap<>();
		EFTlist=new HashMap<>();
	}

	@Override
	public void doschedule(Collection<Task> tasks) {	

		List<Task> sortedTasks = ranktask(tasks);
			
		// Phase 2: Processor Selection
		privateVmSelect(sortedTasks, hybridVMlist.get("private"));
		
		List<Task> EFTsortedTasks=new ArrayList<>(sortedTasks);
		Collections.sort(EFTsortedTasks,new EFTComparator());
		int i=0;
		Task pubTask=EFTsortedTasks.get(i);
		double makespan=EFTlist.get(pubTask);
		List<Task> publist=new ArrayList<>();
		boolean finish=false;
		while(makespan>pubTask.getWorkflow().getDeadline()&&!finish){
Log.printLine("makespan: "+makespan+" dl: "+pubTask.getWorkflow().getDeadline());
			publist.add(pubTask);
			removeFromPrv(pubTask,hybridVMlist.get("private"));
			makespan=updataMakespan(EFTsortedTasks, pubTask, hybridVMlist.get("private"), hybridVMlist.get("public"));			
			
			finish=true;
			Collections.sort(EFTsortedTasks,new EFTComparator());
			for(Task t:EFTsortedTasks){
				if(!publist.contains(t)){
					pubTask=t;
					finish=false;
					break;
				}
			}
		
		}
		taskToPub(publist, hybridVMlist.get("public"));
		
		
	}
	// compute upward ranks of all tasks
	public List<Task> ranktask(Collection<Task> tasks){
		List<Task>sortedTasks=new ArrayList<>(tasks);
		Collections.sort(sortedTasks);
			for (int i = sortedTasks.size() - 1; i >= 0; i--) {
				Task task = sortedTasks.get(i);
				readyTimePerTask.put(task, 0d);
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
							+(task.getCloudletFileSize()+task.getCloudletOutputSize()))/vm.getBw();			
				// note that the upward rank of a task will always be greater than that of its successors
				upwardRanks.put(task, avgcost + maxSuccessorRank);
			}
			
			// Phase 1: Task Prioritizing (sort by decreasing order of rank)
			Collections.sort(sortedTasks, new TaskUpwardRankComparator());
			return sortedTasks;
	}
	public void privateVmSelect(List<Task> sortedTasks, List<HybridVm> vms){
		for (Vm vm : vms) {
			if (!readyTasks.containsKey(vm)) {
				Queue<Task> q = new LinkedList<>();
				readyTasks.put(vm, q);

				TreeSet<Double> occupiedTimeSlotStarts = new TreeSet<>();
				occupiedTimeSlotStarts.add(0d);//因为这里假设每个vm只有一个slot，所以slot的起始时间是0s
				freeTimeSlotStartsPerVm.put(vm, occupiedTimeSlotStarts);
				Map<Double, Double> freeTimeSlotLengths = new HashMap<>();
				freeTimeSlotLengths.put(0d, Double.MAX_VALUE);//因为这里假设每个vm只有一个slot，所以slot的起始时间是0s，终止时间是max
				//如果每个vm不止一个slot，可以设置slot的时长，那这里起止时间就可以有变化
				freeTimeSlotLengthsPerVm.put(vm, freeTimeSlotLengths);//把这个空闲slot加到vm空闲时长列表里面
			}
		}
		for (Task task : sortedTasks) {
			double readyTime = readyTimePerTask.get(task);

			Vm bestVm = null;
			double bestVmFreeTimeSlotActualStart = Double.MAX_VALUE;
			double bestFinish = Double.MAX_VALUE;//记录一个task在所有vm上，能最早完成的时间，把task调度在那个vm上
			double freeTimeSlotActualStart=Double.MAX_VALUE;
			for (Vm vm : vms) {
				double computationCost= task.getCloudletLength()/vm.getCloudletScheduler().getCurrentAvailableRate()
					+(task.getCloudletFileSize()+task.getCloudletOutputSize())/vm.getBw();
			
		//Log.printLine("-计算开销："+computationCost);
			
			TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerVm.get(vm);
			Map<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerVm.get(vm);
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
					}
				}
			}
		
			task.setSubmitTime(CloudSim.clock());
			task.setScheduleTime(CloudSim.clock());
			Map<Vm, Double> vmslot=new HashMap<>();
			vmslot.put(bestVm, freeTimeSlotActualStart);
			taskVmSlot.put(task, vmslot);
			EFTlist.put(task, bestFinish);
			// assign task to vm
			schedule.put(task, bestVm);
			//Log.printLine(CloudSim.clock() + ": " + getName()
				//+ ": Assigning Task # " + task.getCloudletId() + " \""
				//+ task.getName() + " " + task.getParams() + " \""+ " to VM # " + bestVm.getId());

		// update readytime of all successor tasks
		//更新后继节点的最早完成时间，因为传输时间=0，最早完成时间理论上=该task的finishtime
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
				if (bestFinish > readyTimePerTask.get(child)) {
					readyTimePerTask.put(child, bestFinish);
				}
			}

			double timeslotStart = freeTimeSlotStartsPerVm.get(bestVm).floor(bestVmFreeTimeSlotActualStart);
			double timeslotLength = freeTimeSlotLengthsPerVm.get(bestVm).get(timeslotStart);
			double diff = bestVmFreeTimeSlotActualStart - timeslotStart;
		// add time slots before and after
		//如果是“slot开始~task执行~slot结束”的情况，要把后半部分去掉
			if (bestVmFreeTimeSlotActualStart > timeslotStart) {
				freeTimeSlotLengthsPerVm.get(bestVm).put(timeslotStart, diff);
			} 
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
	
	
	public void taskToPub(List<Task> tasks, List<HybridVm> vms){
		for(Task task:tasks){
			for(Vm vm:vms){
				if(vm.getNumberOfPes()>task.getNumberOfPes())
				{
					schedule.put(task, vm);
					task.setSubmitTime(CloudSim.clock());
					task.setScheduleTime(CloudSim.clock());
					break;
				}
			}
		}
	}
	
	public void removeFromPrv(Task task, List<HybridVm> privates){
		Vm vm=schedule.remove(task);

		//double slotstart=taskVmSlot.get(task).get(vm);
		//put slot back to private vms 
	//	Map<Vm, Double> vmslot=new HashMap<>();
		
	}
	public double updataMakespan(List<Task> sortedTasks, Task root, List<HybridVm> privates, List<HybridVm> publics){
		double makespan=readyTimePerTask.get(root);
//Log.printLine("root"+root.getCloudletId()+"-index-"+sortedTasks.indexOf(root)+"-ready--"+makespan);
		for(int i=0;i<=sortedTasks.indexOf(root);i++){						
			Task t=sortedTasks.get(i);
//Log.printLine(root.getCloudletId()+" processor"+t.getCloudletId()+"--"+EFTlist.get(t));
			double maxReady=0.0;
			for (DataDependency inEdge : t.getWorkflow().getGraph().getInEdges(t)) {
				Task parent = t.getWorkflow().getGraph().getSource(inEdge);
				double tempReady=EFTlist.get(parent);
//Log.printLine("parent: "+parent.getCloudletId()+"--"+tempReady);
				if(tempPrivate(parent)&&tempPrivate(t))
					tempReady+=parent.getCloudletOutputSize()/schedule.get(parent).getBw();
				else{
					PrivateDatacenter hdc=(PrivateDatacenter)hybridVMlist.get("public").get(0).getHost().getDatacenter();
					tempReady+=parent.getCloudletOutputSize()/hdc.getOutBw();
				}
				if(maxReady<tempReady)
					maxReady=tempReady;
			}
//Log.printLine(maxReady);
			makespan+=t.getCloudletLength()/publics.get(0).getCloudletScheduler().getCurrentAvailableRate();
			EFTlist.put(t, makespan);
			for (DataDependency outEdge : t.getWorkflow().getGraph().getOutEdges(t)) 
				readyTimePerTask.put(t.getWorkflow().getGraph().getSource(outEdge), makespan);
		}
		return makespan;
	}
	public boolean tempPrivate(Task task){
		if(schedule.containsKey(task))
			return true;
		return false;
	}
}
