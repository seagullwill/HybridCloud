package de.huberlin.wbi.dcs;

import java.util.List;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;

public class PrivateDatacenter extends Datacenter{
	private double lastMonitor;
	public PrivateDatacenter(String name, DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList, double schedulingInterval)
					throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		// TODO Auto-generated constructor stub
		setLastMonitor(0.0);
	}
	@Override
	protected void processVmCreate(SimEvent ev, boolean ack) {
		Vm vm = (Vm) ev.getData();

		boolean result = getVmAllocationPolicy().allocateHostForVm(vm);

		if (ack) {
			int[] data = new int[3];
			data[0] = getId();
			data[1] = vm.getId();

			if (result) {
				data[2] = CloudSimTags.TRUE;
			} else {
				data[2] = CloudSimTags.FALSE;
			}
			send(vm.getUserId(), 0.1, CloudSimTags.VM_CREATE_ACK, data);
		}

		if (result) {
			double amount = 0.0;
			if (getDebts().containsKey(vm.getUserId())) {
				amount = getDebts().get(vm.getUserId());
			}
			amount += getCharacteristics().getCostPerMem() * vm.getRam();
			amount += getCharacteristics().getCostPerStorage() * vm.getSize();

			getDebts().put(vm.getUserId(), amount);

			getVmList().add(vm);
			
			if (vm.isBeingInstantiated()) {
				vm.setBeingInstantiated(false);
			}

			vm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(vm).getVmScheduler()
					.getAllocatedMipsForVm(vm));
		}

	}
	@Override
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		updateCloudletProcessing();

		try {
			// gets the Cloudlet object
			Task task = (Task) ev.getData();

			// checks whether this Cloudlet has finished or not
			if (task.isFinished()) {
				String name = CloudSim.getEntityName(task.getUserId());
				Log.printLine(getName() + ": Warning - Cloudlet #" + task.getCloudletId() + " owned by " + name
						+ " is already completed/finished.");
				Log.printLine("Therefore, it is not being executed again");
				Log.printLine();
				if (ack) {
					int[] data = new int[3];
					data[0] = getId();
					data[1] = task.getCloudletId();
					data[2] = CloudSimTags.FALSE;

					// unique tag = operation tag
					int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
					sendNow(task.getUserId(), tag, data);
				}
				sendNow(task.getUserId(), CloudSimTags.CLOUDLET_RETURN, task);
				return;
			}

			// process this Cloudlet to this CloudResource
			task.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
					.getCostPerBw());			
			int vmId = allocateTask(task);
			task.setVmId(vmId);
			Vm vm =VmList.getById(getVmList(), vmId);
			//Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "+ vmId + 
			//		" in "+vm.getHost().getDatacenter().getName()+" starts executing Task # "
			//		+ task.getCloudletId() + " \"" + task.getName() + " "+ task.getParams() + " \"");
			
			double fileTransferTime = predictFileTransferTime(task,vm);
			CloudletScheduler scheduler = vm.getCloudletScheduler();
			double estimatedFinishTime = scheduler.cloudletSubmit(task, fileTransferTime);

			// if this cloudlet is in the exec queue
			if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
				estimatedFinishTime += fileTransferTime;
				send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
			}

			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = task.getCloudletId();
				data[2] = CloudSimTags.TRUE;

				// unique tag = operation tag
				int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
				sendNow(task.getUserId(), tag, data);
			}
		} catch (ClassCastException c) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
			c.printStackTrace();
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
		}

		checkCloudletCompletion();
	}
	public int allocateTask(Task task){
		int vmId=0;
		double bestFinish=Double.MAX_VALUE;
		for(Vm vm:getVmList()){
			double est=0.0;
			for (DataDependency inEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(inEdge);
				long filesize=task.getInputFilelist().get(parent).getSize();
				
				double filetime=0;
				if(vm.getId()!=parent.getVmId()){
					if(inDC(parent.getVmId()))
						filetime=filesize/vm.getBw();
					else
						filetime=filesize/Parameters.OUTBW;
				}
				
			if(est<parent.getFinishTime()+filetime)	
				est=parent.getFinishTime()+filetime;
			}
			double finishvm=Double.max(est,vm.getCloudletScheduler().getNextSlot())+task.getCloudletLength()/vm.getMips();
			if(bestFinish>finishvm){
				bestFinish=finishvm;
				vmId=vm.getId();
			}
		}
		return vmId;
	}
	protected double predictFileTransferTime(Task task,HybridVm vm) {
		double maxTaskStart=0.0;
		double maxParentFinish=0.0;
		for (DataDependency inEdge : task.getWorkflow().getGraph().getInEdges(task)) {
			Task parent = task.getWorkflow().getGraph().getSource(inEdge);
			
			long filesize=task.getInputFilelist().get(parent).getSize();
			
			double filetime=0;
			if(vm.getId()!=parent.getVmId()){
				if(inDC(parent.getVmId()))
					filetime=filesize/vm.getBw();
				else
					filetime=filesize/Parameters.OUTBW;
			}
			
			double parentfinish=parent.getFinishTime();
			if(maxTaskStart<parentfinish+filetime)
				maxTaskStart=parentfinish+filetime;
			if(maxParentFinish<parentfinish)
				maxParentFinish=parentfinish;
		}
		//Log.printLine(task.getCloudletId()+" in "+task.getCloudletFileSize()+" out "+task.getCloudletOutputSize() +" time "+time+"------");
		return maxTaskStart-maxParentFinish;
	}

	
	public boolean inDC(int vmId){
		if (VmList.getById(getVmList(), vmId)!=null)
			return true;
		return false;
	}
	@Override
	protected void updateCloudletProcessing() {
		if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.001) {
			List<? extends Host> list = getVmAllocationPolicy().getHostList();
			double smallerTime = Double.MAX_VALUE;
			// for each host...
			for (int i = 0; i < list.size(); i++) {
				Host host = list.get(i);
				// inform VMs to update processing
				double time = host.updateVmsProcessing(CloudSim.clock());
				// what time do we expect that the next cloudlet will finish?
				if (time < smallerTime) {
					smallerTime = time;
				}
			}
			// gurantees a minimal interval before scheduling the event
			if (smallerTime < CloudSim.clock() + 0.11) {
				smallerTime = CloudSim.clock() + 0.11;
			}
			if (smallerTime != Double.MAX_VALUE) {
				schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
				if(CloudSim.clock()-getLastMonitor()>Parameters.updateFrequency){
					sendNow(getVmList().get(0).getUserId(), CloudSimTags.NextCycle, 0);	
					setLastMonitor(CloudSim.clock());
				}
			}
			setLastProcessTime(CloudSim.clock());
		}
	}
	public double getLastMonitor() {
		return lastMonitor;
	}
	public void setLastMonitor(double lastMonitor) {
		this.lastMonitor = lastMonitor;
	}
}
