package de.huberlin.wbi.dcs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.CloudletSchedulerHybrid;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class HybridDCBroker extends DatacenterBroker{
	protected List<Workflow> workflows;
	private int taskSlotsPerVm;
	protected Queue<Vm> idleTaskSlots;
	protected double cost;
	protected long moveData;
	// two collections of tasks, which are currently running;
	// note that the second collections is a subset of the first collection
	private Map<Integer, Task> tasks;
	public ArrayList<String> Result;
	protected Map<Task, Vm> schedule;
	protected Map<Vm, Queue<Task>> readyTasks;
	public Map <String, List<HybridVm>> hybridVMlist;
	public HybridDCBroker(String name, int taskSlotsPerVm) throws Exception {
		super(name);
		workflows = new ArrayList<>();
		this.taskSlotsPerVm = taskSlotsPerVm;
		idleTaskSlots = new LinkedList<>();
		tasks = new HashMap<>();

		schedule = new HashMap<>();
		readyTasks = new HashMap<>();
		hybridVMlist=new HashMap<>();	
		Result=new ArrayList<>();
		cost=0.0;
		moveData=0;

	}
	
	public List<Workflow> getWorkflows() {
		return workflows;
	}

	public int getTaskSlotsPerVm() {
		return taskSlotsPerVm;
	}

	public void submitWorkflow(Workflow workflow) {
		workflows.add(workflow);
	}

	//overrided
	@Override
	protected void submitCloudlets() {
		for (Vm vm : getVmsCreatedList()) {
			for (int i = 0; i < getTaskSlotsPerVm(); i++) {
				idleTaskSlots.add(vm);
			}
		}
		for (Workflow workflow : workflows) {
			doschedule(workflow);			
			for (Task task : workflow.getTasks()) {
				if (task.readyToExecute()) {
					taskReady(task);
				}
			}
		}
		
		Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		while (tasksRemaining() && !idleTaskSlots.isEmpty()) {
			Vm vm = idleTaskSlots.remove();
			Task task = getNextTask(vm);
			if (task == null) 
				taskSlotsKeptIdle.add(vm);
			 else {
				tasks.put(task.getCloudletId(), task);
				double time=CloudSim.clock()+task.getSubmitTime();
				Log.printLine(time + ": " + getName() + ": VM # "+ vm.getId() + 
						" in "+vm.getHost().getDatacenter().getName()+" starts executing Task # "
						+ task.getCloudletId() + " \"" + task.getName() + " "+ task.getParams() + " \"");
				task.setScheduleTime(time);
				task.setVmId(vm.getId());
				send(getVmsToDatacentersMap().get(vm.getId()),time,CloudSimTags.CLOUDLET_SUBMIT, task);
			}
		}
		idleTaskSlots.addAll(taskSlotsKeptIdle);
	}

	protected void submitTasks() {
		Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		while (tasksRemaining() && !idleTaskSlots.isEmpty()) {
			Vm vm = idleTaskSlots.remove();
			Task task = getNextTask(vm);
			if (task == null) 
				taskSlotsKeptIdle.add(vm);
			 else {
				tasks.put(task.getCloudletId(), task);
				submitTask(task, vm);
			}
		}
		idleTaskSlots.addAll(taskSlotsKeptIdle);
	}
	private void submitTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "+ vm.getId() + 
				" in "+vm.getHost().getDatacenter().getName()+" starts executing Task # "
				+ task.getCloudletId() + " \"" + task.getName() + " "+ task.getParams() + " \"");
		task.setScheduleTime(CloudSim.clock());
		task.setVmId(vm.getId());
		
		sendNow(getVmsToDatacentersMap().get(vm.getId()),CloudSimTags.CLOUDLET_SUBMIT, task);
		//sendNow(datacenterIdsList.get(0),CloudSimTags.CLOUDLET_SUBMIT, task);
		
	}
	protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			if (vm instanceof HybridVm) {
				HybridVm dVm = (HybridVm) vm;
				dVm.closePerformanceLog();
			}
		}
		super.clearDatacenters();
	}
	protected void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
			task.setCloudletStatus(Cloudlet.CREATED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished,
		Task task = (Task) ev.getData();
		HybridVm vm = VmList.getById(getVmList(), task.getVmId());
		Host host = vm.getHost();

		tasks.remove(task.getCloudletId());
		Log.printLine(CloudSim.clock() +": "+ getName() +": VM ## "+task.getVmId() +
						"in "+host.getDatacenter().getName()+" completed Task # "
						+ task.getCloudletId() + " \""+ task.getName() + " "+ task.getParams() + " \"");				
				// free task slots occupied by finished / cancelled tasks
		idleTaskSlots.add(vm);
			
			// update the task queue by traversing the successor nodes in the workflow
		for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
			if (host instanceof HybridHost) {
				HybridHost dHost = (HybridHost) host;
				dHost.addFile(outgoingEdge.getFile());
			}
			Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
			child.decNDataDependencies();
			if (child.readyToExecute()) 
				taskReady(child);
			}

		if (tasksRemaining()) {
			submitTasks();
		} else if (idleTaskSlots.size() == getVmsCreatedList().size()* getTaskSlotsPerVm()) {					
			Log.printLine(CloudSim.clock() + ": " + getName()+ ": All Tasks executed. Finishing...");
			terminate();
			clearDatacenters();
			finishExecution();
		}
}

	public Task getNextTask(Vm vm) {
		Queue<Task> tasks = readyTasks.get(vm);
		if (tasks!=null && tasks.size() > 0) {
			return tasks.remove();
		}
		return null;
	}
	public void doschedule(Workflow w) {
		
	}
	
	public void taskFailed(Task task, Vm vm) {
	}

	
	public void taskReady(Task task) {
		readyTasks.get(schedule.get(task)).add(task);
	}

	
	public boolean tasksRemaining() {
		for (Queue<Task> queue : readyTasks.values()) {
			if (!queue.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	
	public void terminate() {
	}
	
	public Vm getScheduleVm(Task task) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	protected void processResourceCharacteristics(SimEvent ev) {
		DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
		getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

		if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {		
			setDatacenterRequestedIdsList(new ArrayList<Integer>());	
			
			createVmsInDatacenter();
		}
	}
	protected void createVmsInDatacenter() {
		int requestedVms = 0;
		for(int i=0;i<getDatacenterIdsList().size();i++){
			int datacenterId=getDatacenterIdsList().get(i);
			String datacenterName = CloudSim.getEntityName(datacenterId);
			if(datacenterName.contains("private")){
				long storage = 10000;
				String vmm = "Xen";
				int j = 0;
				HybridVm vm;
				List <HybridVm> list=new ArrayList<HybridVm>();
				DynamicModel dynamicModel = new DynamicModel();
				for (; j < 2*Parameters.nVms; j++) {	
					vm = new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 1,
							2*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,true, 0.0);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//	+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				for (; j < 3*Parameters.nVms; j++) {
					vm= new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 2,
							4*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,true,0.0);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//				+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				for (; j < 4*Parameters.nVms; j++) {
					vm= new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 4,
							16*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,true,0.0);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//			+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				for (; j < 5*Parameters.nVms; j++) {
					vm= new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 8,
							32*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,true,0.0);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//			+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				hybridVMlist.put("private", list);
			}
			if(datacenterName.contains("public")){
				long storage = 10000;
				String vmm = "Xen";
				int j = 0;
				HybridVm vm;
				List <HybridVm> list=new ArrayList<HybridVm>();
				DynamicModel dynamicModel = new DynamicModel();

				for (j=1000; j < 2000; j++) {					
					vm = new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 1,
							2*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,false,0.032);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//	+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				for (; j < 3000; j++) {
					vm= new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 2,
							4*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,false,0.064);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//				+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				for (; j < 3500; j++) {
					vm = new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 4,
							16*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,false,0.256);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//			+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				for (; j < 4000; j++) {
					vm = new HybridVm(j, getId(), Parameters.numberOfCusPerPe, 8,
							32*1024, storage, vmm, new CloudletSchedulerHybrid(),//这里可以改cloudletScheduler的方式
							dynamicModel, "output/run_vm_" + i + ".csv",//找到这个文件输出的位置
							Parameters.taskSlotsPerVm,false,0.512);
				//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
				//			+ " in "+ datacenterId+" - " + datacenterName);
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
					requestedVms++;
					submitVM(vm);
					list.add(vm);
				}
				hybridVMlist.put("public", list);
			}
			getDatacenterRequestedIdsList().add(datacenterId);
		}
		
		setVmsRequested(requestedVms);
		setVmsAcks(0);
	}
	
	private void submitVM(Vm vm){
		getVmList().add(vm);
	}
	@Override
	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + CloudSim.getEntityName(datacenterId) + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
		//Log.printLine(vmId+"..."+VmList.getById(getVmsCreatedList(), vmId).getMips()+"...."
			//		+VmList.getById(getVmsCreatedList(), vmId).getNumberOfPes()+"..."+
				//	+VmList.getById(getVmsCreatedList(), vmId).getBw());
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
					+ " failed in Datacenter #" + datacenterId);			
		}

		incrementVmsAcks();

		// all the requested VMs have been created
		if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
			for(String dcName:hybridVMlist.keySet()){
				List<HybridVm> list=hybridVMlist.get(dcName);
				for(Vm vmTod:list){
					if( VmList.getById(getVmsCreatedList(),vmTod.getId())==null)
						list.remove(vmTod);
				}
				hybridVMlist.put(dcName, list);
			}
			//workflows.get(0).computeDeadline(getVmsCreatedList().get(0).getMips()*getVmsCreatedList().get(0).getNumberOfPes(),
		//				getVmsCreatedList().get(0).getBw());
		//	double dl=workflows.get(0).getDeadline();
			double dl;
			switch (Parameters.experiment) {
				case MONTAGE_100:
					dl=Parameters.dlmontage100;
					break;
				case CYBERSHAKE_100:
					dl=Parameters.dlcybershake100;
					break;
				case CYBERSHAKE_1000:
					dl=Parameters.dlcybershake1000;
					break;
				default:
					workflows.get(0).computeDeadline(getVmsCreatedList().get(0).getMips()*getVmsCreatedList().get(0).getNumberOfPes(),
						getVmsCreatedList().get(0).getBw());
					dl=workflows.get(0).getDeadline();
					break;
			}
					
			for(Workflow w:workflows){
				w.setDeadline(dl);				
				w.setLST( getVmsCreatedList().get(0).getMips()*getVmsCreatedList().get(0).getNumberOfPes());
				//w.TabuPartition(getVmsCreatedList().get(0).getMips()*getVmsCreatedList().get(0).getNumberOfPes(),
				//		getVmsCreatedList().get(0).getBw());
				w.setPlist(getVmsCreatedList().get(0).getMips()*getVmsCreatedList().get(0).getNumberOfPes(),
								getVmsCreatedList().get(0).getBw());
			}
			submitCloudlets();
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
						return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
					submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}

	public void checkTasks(){
		Log.printLine(CloudSim.clock()+"Checking tasks... ...");
	}
	
	public double getCost(){
		return cost;
	}
	public double getMovedata(){
		return moveData;
	}
	public int publicNum(){
		return 0;
	}
}
