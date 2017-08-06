package de.huberlin.wbi.dcs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import de.huberlin.wbi.dcs.HCOC.TaskUpwardRankComparator;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Partition;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class DynamicHybridDCBroker extends HybridDCBroker{

	public class TaskUpwardRankComparator implements Comparator<Task> {
		
		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(upwardRanks.get(task2), upwardRanks.get(task1));
		}
	}
	private List<Task> remainTasks;
	private Map<Task, Double> upwardRanks;
	protected List<Task> readyTasks;
	private ArrayList<Task> pubTasks;
	private ArrayList<Workflow> checked;
	public DynamicHybridDCBroker(String name, int taskSlotsPerVm) throws Exception {
		super( name,  taskSlotsPerVm);
		remainTasks=new ArrayList<>();
		readyTasks = new ArrayList<>();
		upwardRanks = new HashMap<>();
		pubTasks=new ArrayList<>();
		checked=new ArrayList<>();
	}

	@Override
	protected void submitCloudlets() {
		 Collections.sort(workflows,new Comparator<Workflow>(){  
	            @Override  
	            public int compare(Workflow w1, Workflow w2) {  
	                return Double.compare(w1.getDeadline(),w2.getDeadline());  
	            }  
	        });
		Map<Workflow,Double> rankWorkflow=new HashMap<>();
		for (Workflow workflow : workflows) {//Log.printLine(workflow.getDeadline()+"-------");
			List<Task> sortedTasks = ranktask(workflow.getTasks());
			workflow.setSortedTask(sortedTasks);
			//目前根本没有考虑数据传输问题，也没有考虑dependency吧？用collection拿到workflow里的task
			
		//	rankWorkflow.put(workflow, upwardRanks.get(sortedTasks.get(0)));
		}
		/*Collections.sort(workflows,new Comparator<Workflow>(){  
            @Override  
            public int compare(Workflow w1, Workflow w2) {  
                return Double.compare(rankWorkflow.get(w2),rankWorkflow.get(w1));  
            }  
        });*/
		for (Workflow workflow : workflows){//Log.printLine(workflows.indexOf(workflow)+"-"+rankWorkflow.get(workflow));
			remainTasks.addAll(workflow.getSortedTask());
			if(workflow.getSubmitTime()<=CloudSim.clock())
				submitworkflow();
			else
				send(getId(),workflow.getSubmitTime(),CloudSimTags.NewWorkflow, 0);		}
	}
	private void submitworkflow(){
		for (Task task : remainTasks) 
			if (task.readyToExecute(CloudSim.clock())) 
				readyTasks.add(task);
		submitTasks();	
	}
	@Override
	protected void submitTasks() {	
		for (Task task:readyTasks) {
			task.setScheduleTime(CloudSim.clock());
			remainTasks.remove(task);
			int dcId=2;
			if(pubTasks.contains(task)){
				task.setPrivate(false);
				dcId=getVmsToDatacentersMap().get(hybridVMlist.get("public").get(0).getId());
			}
			else
				dcId=getVmsToDatacentersMap().get(hybridVMlist.get("private").get(0).getId());
					sendNow(dcId,CloudSimTags.CLOUDLET_SUBMIT, task);
		}
		readyTasks.clear();
	}
	
	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished,
		Task task = (Task) ev.getData();		
		
		HybridVm vm = VmList.getById(getVmList(), task.getVmId());

		Log.printLine(CloudSim.clock() +": "+ getName() +": VM # "+task.getVmId() + "completed Task # "
						+ task.getCloudletId() + " \""+ task.getName() + " "+ task.getParams() + " \"");	
		String result=task.getCloudletId()+","+task.getSubmitTime()+","+task.getScheduleTime()+","+task.getExecStartTime()+","
						+task.getFinishTime()+","+task.getVmId()+","+task.getCloudletLength()+","+(task.getCloudletFileSize()+task.getCloudletOutputSize());
		if(vm.isPrivate())
			result+=",private";
		else
			result+=",public";
		Result.add(result);
		long outsize=0;
		for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
			Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
			child.decNDataDependencies();
			if(child.getEst()<CloudSim.clock())
				child.setEst(CloudSim.clock());
			if (child.readyToExecute()) 
				readyTasks.add(child);
			}
		for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
			Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
//Log.printLine(task.isPrivate()+"----------------------"+parent.isPrivate());
			
			if(!(task.isPrivate()&&parent.isPrivate())){		
				moveData+=task.getInputFilelist().get(parent).getSize();
				if(!parent.isPrivate())
					outsize+=task.getInputFilelist().get(parent).getSize();
				}		
			}		
		cost+=vm.getPrice()*(task.getFinishTime()-task.getScheduleTime())+outsize*Parameters.priceData;
		//Log.printLine(vm.getPrice()+"-"+task.getFinishTime()+"-"+task.getScheduleTime()+"-"+outsize);
			
		/*for(Task remain:remainTasks){
			if(remain.readyToExecute(CloudSim.clock())&&!readyTasks.contains(remain))
				readyTasks.add(remain);
		}*/
		if (remainTasks.isEmpty()) {
			Log.printLine(CloudSim.clock() + ": " + getName()+ ": All Tasks executed. Finishing...");
			terminate();
			clearDatacenters();
			finishExecution();
		}
		else
			submitTasks();
	}
	
	@Override
	protected void processOtherEvent(SimEvent ev) {
		if (ev.getTag() == CloudSimTags.NextCycle) {
			//doSubworkflowData();
			//doTaskD();
			doSubworkflowD();
			//doSubworkflowDataAll();
			//doWorkflowD();
Log.printLine(CloudSim.clock()+":"+getName() + ".cycle");
			
			return;
		}
		if (ev.getTag() == CloudSimTags.NewWorkflow){
			//submitworkflow((Workflow)ev.getData());
			submitworkflow();
			return;
		}
			
		Log.printLine(CloudSim.clock()+":"+getName() + ".processOtherEvent(): "+ "Error - event unknown by this DatacenterBroker.");
	}
	
	public void doSubworkflowData(){
		List<Task> move=new ArrayList<>();
		Task point=checkDL(0);
		while(point!=null){
			move.clear();	
			long mincut=point.getCloudletFileSize()+point.getCloudletOutputSize();
			if(point.getPartition()!=null)  mincut=pubCut(point.getPartition());
			long cut=0;
			for(Partition p:point.getWorkflow().getExcPlist()){//
				if(pBeforet(p,point)
						&&p.getLength()>=point.getCloudletLength()
						&&(!pubTasks.contains(p.getTasks().get(0)))){
					cut=pubCut(p);
					if(mincut>cut){
						mincut=cut;
						move.clear();
						move.addAll(p.getTasks());
					}
				}	
			}
			
			if(move.size()==0){
				if(point.getPartition()!=null)
					move.addAll(point.getPartition().getTasks());
				else
					move.add(point);
			}
for(Task t:move)
	Log.print(t.getCloudletId()+"-");
Log.printLine();long pointcut=0;
if(point.getPartition()!=null)  pointcut=pubCut(point.getPartition());
long length=0;
if(move.get(0).getPartition()!=null) length=move.get(0).getPartition().getLength();
Log.printLine("cut:"+mincut+"-p-"+pointcut+"-movelength-"+length+"-pl-"+point.getCloudletLength()+"id:"
+point.getCloudletId()+"-"+workflows.indexOf(point.getWorkflow()));
			pubTasks.addAll(move);
			point=checkDL(0);
			}
	}
	public void doSubworkflowDataAll(){
		List<Task> move=new ArrayList<>();
		
		int old=-1;
		int i=0;
		Task point=checkDL(i);
		while(point!=null){
			if(workflows.indexOf(point.getWorkflow())==old)
				i+=move.size();
			else
				i=0;
			old=workflows.indexOf(point.getWorkflow());
			move.clear();
			long mincut=Long.MAX_VALUE;
			long cut=0;
			Log.printLine("id:"+point.getCloudletId()+"-"+workflows.indexOf(point.getWorkflow()));
			for(int j=0; j<workflows.indexOf(point.getWorkflow());j++){
				for(Partition p:workflows.get(j).getExcPlist()){
					if(p.getLength()>=point.getCloudletLength()&&(!pubTasks.contains(p.getTasks().get(0)))){
						cut=pubCut(p);
						if(mincut>cut){
							mincut=cut;
							move.clear();
							move.addAll(p.getTasks());
						}
					}
				}	
			}
				
			for(Partition p:point.getWorkflow().getExcPlist()){
				if(pBeforet(p,point)&&p.getLength()>=point.getCloudletLength()&&(!pubTasks.contains(p.getTasks().get(0)))){
					cut=pubCut(p);
					if(mincut>cut){
						mincut=cut;
						move.clear();
						move.addAll(p.getTasks());
					}
				}	
			}
			
			if(move.size()==0){
				if(point.getPartition()!=null)
					move.addAll(point.getPartition().getTasks());
				else
					move.add(point);
			}
			for(Task t:move)
				Log.print(t.getCloudletId()+"-");
			Log.printLine();long pointcut=0;
			if(point.getPartition()!=null)  pointcut=pubCut(point.getPartition());
			long length=0;
			if(move.get(0).getPartition()!=null) length=move.get(0).getPartition().getLength();
			Log.printLine("cut:"+mincut+"-p-"+pointcut+"-movelength-"+length+"-pl-"+point.getCloudletLength()+"w:"+workflows.indexOf(move.get(0).getWorkflow()));

			pubTasks.addAll(move);
			point=checkDL(i);
			}
	}
	public void doSubworkflowD(){
		Task point=checkDL(0);
		while(point!=null){
			if(point.getPartition()==null)
				pubTasks.add(point);
			else {
				if(point.getPartition().getTasks()!=null){
					for(Task task:point.getPartition().getSortedTasks())
						if(task.getStatus()==0&&(!pubTasks.contains(task)))
							pubTasks.add(task);
				}
			}	
			point=checkDL(0);//checkDeadline();
			}
	}
	public void doTaskD(){
		Task point=checkDL(0);
		while(point!=null){
			long mincut=point.getCloudletOutputSize()+point.getCloudletFileSize();
			int tempIndex=remainTasks.indexOf(point);
			for(Task t:point.getWorkflow().getSortedTasks()){
				if((!pubTasks.contains(t))&&t.getStatus()==0
						&&remainTasks.indexOf(t)<=remainTasks.indexOf(point)
						&&t.getCloudletLength()>=point.getCloudletLength()){
					long cut=t.getCloudletOutputSize()+t.getCloudletFileSize();
					if(mincut>=cut){
						mincut=cut;
						tempIndex=remainTasks.indexOf(t);//Log.printLine(tempIndex+"-"+t.getCloudletId());
					}
				}
			}
			pubTasks.add(remainTasks.get(tempIndex));
			//pubTasks.add(point);
			
			point=checkDL(0);
			}
	}
	public void doWorkflowD(){
		List<Task> move=new ArrayList<>();
		Task point=checkDL(0);
		
		if(point!=null){
			for(Task task:point.getWorkflow().getSortedTasks())
				if(task.getStatus()==0&&(!pubTasks.contains(task)))
					move.add(task);
					
			pubTasks.addAll(move);
			point=checkDL(0);
			}
	}

	public long SumCut(List<Task> tasks){
		long weight=0;
		Map<Task,Long> EFT=new HashMap();
		
		for (Task task:tasks) {
			long maxest = 0;
			for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
				if(EFT.containsKey(parent)){
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
				upwardRanks.put(task, avgcost + maxSuccessorRank);
			}
			Collections.sort(sortedTasks, new TaskUpwardRankComparator());
			return sortedTasks;
	}

	public Task checkDeadline(){
		Map <Integer, Double> EFT=new HashMap<>();
		double pricompute=0.0;
		double pubcompute=0.0;
		for(Vm vm:getVmList()){
			EFT.putAll(vm.getCloudletScheduler().getExecTimeList());
			//CloudSim.getEntityName(vmsToDatacentersMap.get(vm.getId()).)
			if(hybridVMlist.get("private").contains(vm))
				pricompute+=vm.getCloudletScheduler().getCurrentAvailableRate();
			if(hybridVMlist.get("public").contains(vm))
				pubcompute+=vm.getCloudletScheduler().getCurrentAvailableRate();
		}
		pricompute/=hybridVMlist.get("private").size();
		pubcompute/=hybridVMlist.get("public").size();
		for(Task task:remainTasks){
			double maxest = Math.max(CloudSim.clock(), task.getWorkflow().getSubmitTime());
			for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
						
				double transferTime=0;
				if(pubTasks.contains(parent)&&(!pubTasks.contains(task)))	
					transferTime=task.getInputFilelist().get(parent).getSize()/Parameters.OUTBW;
				else{
					if((!pubTasks.contains(parent)&&(!pubTasks.contains(task))))
						transferTime=task.getInputFilelist().get(parent).getSize()/
							getVmsCreatedList().get(0).getBw();
				}
					
				if(EFT.containsKey(parent.getCloudletId()))
					if (maxest<EFT.get(parent.getCloudletId())+transferTime) 
						maxest = EFT.get(parent.getCloudletId())+transferTime;
			}
			if(pubTasks.contains(task))
				maxest+=task.getCloudletLength()/pubcompute;
			else{
				maxest+=task.getCloudletLength()/pricompute;
				if(maxest>task.getLst()){
				//	if(i<=waitingsize(task.getWorkflow()))//&&(!checked.contains(task.getWorkflow())))
	//Log.printLine(task.getCloudletId()+" eft: "+maxest+" lst: "+task.getLst());
						return task;
					//else
					//	checked.add(task.getWorkflow());
				}
				//if(maxest>task.getLst()&&i<=waitingsize(task.getWorkflow()))
				//	return task;
			}
			EFT.put(task.getCloudletId(), maxest);
	//Log.printLine(task.getCloudletId()+" eft: "+EFT.get(task.getCloudletId()));
			}
		return null;
	}
	
	@Override
	public boolean tasksRemaining(){
		return !readyTasks.isEmpty();
	}
	@Override
	public int publicNum(){
		return pubTasks.size();
	}
	public int waitingsize(Workflow w){
		int i=0;
		for(Task t:w.getTasks())
			if(t.getStatus()==0&&(!pubTasks.contains(t)))
				i++;
		return i;
	}
	
	private boolean pBeforet(Partition p, Task point){
		for(Task parent : point.getWorkflow().getSortedParent(point)){
			if(p.containTask(parent))
				return true;
			else
				return pBeforet(p,parent);
		}
		return false;
	}

	public long pubCut(Partition p){
		long cut=0;
		for(Task t:p.getTasks()){
			for(Task f:t.getInputFilelist().keySet()){
				if((!p.containTask(f))&&(!pubTasks.contains(f)))
					cut+=t.getInputFilelist().get(f).getSize();
			}
			for(Task f:t.getOutputFilelist().keySet()){
				if((!p.containTask(f))&&(!pubTasks.contains(f)))
					cut+=t.getOutputFilelist().get(f).getSize();
			}
		}
		return cut;
	}
	public Task checkDL(int i){
		Map<Vm, TreeSet<Double>> freeTimeSlotStartsPerVm=new HashMap<>();//vm的空闲slot的起始时间点，为什么是一个二叉树，可能只是为了排序方便？
		Map<Vm, Map<Double, Double>> freeTimeSlotLengthsPerVm=new HashMap<>();//vm的空闲slot时长<vm，<slot起点，slot时长>>
		Map <Integer, Double> EFT=new HashMap<>();
		double pricompute=0.0;
		double pubcompute=0.0;
		for(Vm vm:getVmList()){
			TreeSet<Double> starts = new TreeSet<>();
			starts.add(vm.getCloudletScheduler().getNextSlot());
			freeTimeSlotStartsPerVm.put(vm, starts);
			
			Map<Double, Double> lengths=new HashMap<>();
			lengths.put(vm.getCloudletScheduler().getNextSlot(),Double.MAX_VALUE);
			freeTimeSlotLengthsPerVm.put(vm, lengths);
			
			EFT.putAll(vm.getCloudletScheduler().getExecTimeList());
			if(hybridVMlist.get("private").contains(vm))
				pricompute+=vm.getCloudletScheduler().getCurrentAvailableRate();
			if(hybridVMlist.get("public").contains(vm))
				pubcompute+=vm.getCloudletScheduler().getCurrentAvailableRate();
		}
		pricompute/=hybridVMlist.get("private").size();
		pubcompute/=hybridVMlist.get("public").size();
		
			// Phase 2: Processor Selection
			for (Task task : remainTasks) {
				double readyTime = Math.max(CloudSim.clock(),task.getWorkflow().getSubmitTime());
				Vm bestVm = null;
				double bestVmFreeTimeSlotActualStart = Double.MAX_VALUE;
				double bestFinish = Double.MAX_VALUE;//记录一个task在所有vm上，能最早完成的时间，把task调度在那个vm上
				if(pubTasks.contains(task)){
					for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
						Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
								
						double transferTime=0;
						if(!pubTasks.contains(parent))	
							transferTime=task.getInputFilelist().get(parent).getSize()/Parameters.OUTBW;
						
						if(EFT.containsKey(parent.getCloudletId()))
							if (readyTime<EFT.get(parent.getCloudletId())+transferTime) 
								readyTime = EFT.get(parent.getCloudletId())+transferTime;
					}
					double computationCost=task.getCloudletLength()/pubcompute;
					
					for (Vm vm : hybridVMlist.get("public")) {
						TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerVm.get(vm);
						Map<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerVm.get(vm);
						//找该vm里，在ready time之后第一个空闲slot的开始时间
						SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts.floor(readyTime) != null) 
								? freeTimeSlotStarts.tailSet(freeTimeSlotStarts.floor(readyTime))
								: freeTimeSlotStarts.tailSet(freeTimeSlotStarts.ceiling(readyTime));
						for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {
							//取第一段空闲slot
							double freeTimeSlotLength = freeTimeSlotLengths.get(freeTimeSlotStart);
							//实际可以开始执行的时间点，是空闲slot开始时间&readytime中，比较晚的那个
							double freeTimeSlotActualStart = Math.max(readyTime, freeTimeSlotStart);
							//如果实际开始时间+计算时间晚于目前拿到的最快的完成时间，则结束循环（结束该vm空闲slot查找，然后结束该vm，进行下一个vm）
							
							if (freeTimeSlotActualStart + computationCost > bestFinish)
								break;
							//如果实际开始时间是在空闲slot开始之后，也就是，readytime>空闲slot开始时间，实际空闲slot长度变成readytime~空闲slot停止时间点
							if (freeTimeSlotActualStart > freeTimeSlotStart)
								freeTimeSlotLength -= freeTimeSlotActualStart- freeTimeSlotStart;
							//如果实际空闲slot内，可以计算，则记录目前最好值，进行下一次循环，直到上面满足break条件，表示找到最好的结果
							if (computationCost < freeTimeSlotLength) {
								bestVm = vm;
								bestVmFreeTimeSlotActualStart = freeTimeSlotActualStart;
								bestFinish = freeTimeSlotActualStart + computationCost;
							}
						}
					}
					
					EFT.put(task.getCloudletId(), bestFinish);
					
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
					//把空闲slot里，减掉task占掉的部分，后面的部分加在空闲slot列表里面
					double actualTimeSlotLength = timeslotLength - diff;
					if (computationCost < actualTimeSlotLength) {
						freeTimeSlotStartsPerVm.get(bestVm).add(bestFinish);
						freeTimeSlotLengthsPerVm.get(bestVm).put(bestFinish,actualTimeSlotLength - computationCost);
					}
				}
			else{
				for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
					Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);
						
					double transferTime=0;
					if(pubTasks.contains(parent))	
						transferTime=task.getInputFilelist().get(parent).getSize()/Parameters.OUTBW;
					else
						transferTime=task.getInputFilelist().get(parent).getSize()/
							getVmsCreatedList().get(0).getBw();	
					if(EFT.containsKey(parent.getCloudletId()))
						if (readyTime<EFT.get(parent.getCloudletId())+transferTime) 
							readyTime = EFT.get(parent.getCloudletId())+transferTime;
				}//Log.printLine(task.getCloudletId()+" eft: "+readyTime+" lst: "+task.getLst());
				if(readyTime>task.getLst()){
					if(i<=waitingsize(task.getWorkflow())&&(!checked.contains(task.getWorkflow())))
						return task;
					else
						checked.add(task.getWorkflow());
				}
				double computationCost= task.getCloudletLength()/pricompute;
				for (Vm vm : hybridVMlist.get("private")) {
					TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerVm.get(vm);
					Map<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerVm.get(vm);
					//找该vm里，在ready time之后第一个空闲slot的开始时间
					SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts.floor(readyTime) != null) 
							? freeTimeSlotStarts.tailSet(freeTimeSlotStarts.floor(readyTime))
							: freeTimeSlotStarts.tailSet(freeTimeSlotStarts.ceiling(readyTime));
					for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {
						//取第一段空闲slot
						double freeTimeSlotLength = freeTimeSlotLengths.get(freeTimeSlotStart);
						//实际可以开始执行的时间点，是空闲slot开始时间&readytime中，比较晚的那个
						double freeTimeSlotActualStart = Math.max(readyTime, freeTimeSlotStart);
						//如果实际开始时间+计算时间晚于目前拿到的最快的完成时间，则结束循环（结束该vm空闲slot查找，然后结束该vm，进行下一个vm）
						
						if (freeTimeSlotActualStart + computationCost > bestFinish)
							break;
						//如果实际开始时间是在空闲slot开始之后，也就是，readytime>空闲slot开始时间，实际空闲slot长度变成readytime~空闲slot停止时间点
						if (freeTimeSlotActualStart > freeTimeSlotStart)
							freeTimeSlotLength -= freeTimeSlotActualStart- freeTimeSlotStart;
						//如果实际空闲slot内，可以计算，则记录目前最好值，进行下一次循环，直到上面满足break条件，表示找到最好的结果
						if (computationCost < freeTimeSlotLength) {
							bestVm = vm;
							bestVmFreeTimeSlotActualStart = freeTimeSlotActualStart;
							bestFinish = freeTimeSlotActualStart + computationCost;
						}
					}
				}
				
				EFT.put(task.getCloudletId(), bestFinish);

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
				//把空闲slot里，减掉task占掉的部分，后面的部分加在空闲slot列表里面
				double actualTimeSlotLength = timeslotLength - diff;
				if (computationCost < actualTimeSlotLength) {
					freeTimeSlotStartsPerVm.get(bestVm).add(bestFinish);
					freeTimeSlotLengthsPerVm.get(bestVm).put(bestFinish,actualTimeSlotLength - computationCost);
				}
			}
		}
			return null;
	}	
}
