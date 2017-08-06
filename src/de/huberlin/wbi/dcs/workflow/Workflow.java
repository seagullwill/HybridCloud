package de.huberlin.wbi.dcs.workflow;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.util.*;

import javax.imageio.ImageIO;
//import javax.swing.JFrame;

import org.apache.commons.collections15.Transformer;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ParameterException;
import org.cloudbus.cloudsim.Vm;

import de.huberlin.wbi.dcs.examples.Parameters;
import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.VisualizationImageServer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
//import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
//import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;

public class Workflow {
	
	private Graph<Task, DataDependency> workflow;
	
	private int[] breadth;
	private double deadline;
	private double submitTime;
	private List<Partition> plist;
	private List<Task> sortedTask;
	
	public Workflow() {
		workflow = new DirectedSparseMultigraph<Task, DataDependency>();
		plist=new ArrayList<>();
		setSortedTask(new ArrayList<>());
	}
	
	public void addTask(Task task) {
		workflow.addVertex(task);
	}
	
	public void addFile(File file, Task taskGeneratingThisFile, List<Task> tasksRequiringThisFile) {
		try {
			int depth = taskGeneratingThisFile.getDepth() + 1;
			for (Task t : tasksRequiringThisFile) {
				workflow.addEdge(new DataDependency(file), taskGeneratingThisFile, t);
				t.incNDataDependencies();
				setDepth(t, depth);				
				t.addInputFile(taskGeneratingThisFile,file);
				taskGeneratingThisFile.addOutputFile(t, file);
			}
			
		} catch (ParameterException e) {
			e.printStackTrace();
		}
	}
	
	private void computeWidthArray() {
		breadth = new int[Task.getMaxDepth()+1];
		for (Task t : workflow.getVertices()) {
			int depth = t.getDepth();
			breadth[depth]++;
		}
	}
	
	private void setDepth(Task task, int depth) {
		if (task.getDepth() < depth) {
			task.setDepth(depth);
			for (Task successor : workflow.getSuccessors(task)) {
				setDepth(successor, depth + 1);
			}
		}
	}
	
	public Graph<Task, DataDependency> getGraph() {
		return workflow;
	}
	
	public Collection<Task> getTasks() {
		return workflow.getVertices();
	}
	
	public List<Task> getSortedTasks() {
		List<Task> tasks = new ArrayList<Task>(getTasks());
		Collections.sort(tasks);
		return tasks;
	}
	public Collection<Task> getChildren(Task task) {
		return workflow.getSuccessors(task);
	}
	public Collection<Task> getParent(Task task) {
		return workflow.getPredecessors(task);
	}
	public List<Task> getSortedChildren (Task task) {
		List<Task> children = new ArrayList<Task>(getChildren(task));
		Collections.sort(children);
		return children;
	}
	public List<Task> getSortedParent (Task task) {
		List<Task> parent = new ArrayList<Task>(getParent(task));
		Collections.sort(parent);
		return parent;
	}
	
	public int getNTasks() {
		return workflow.getVertexCount();
	}
	
	@Override
	public String toString() {
		return workflow.toString();
	}
	
	public void visualize(int width, int height) {
		computeWidthArray();
		Dimension size = new Dimension(width, height);

		StaticLayout<Task, DataDependency> sl = new StaticLayout<Task, DataDependency>(workflow, new TaskPositionInWorkflowTransformer(width, height, breadth));
		sl.setSize(size);
		VisualizationViewer<Task, DataDependency> vv = new VisualizationViewer<Task, DataDependency>(sl);
		vv.setPreferredSize(size); 
		
		// Transformer maps the vertex number to a vertex property
        Transformer<Task,Paint> vertexColor = new Transformer<Task,Paint>() {
            public Paint transform(Task task) {
            	switch (task.getDepth()) {
            		case 2: return new Color(31,73,125);   // index
            		case 3: return new Color(228,108,10);  // align
            		case 4: return new Color(119,147,60);  // samtools view
            		case 5: return new Color(244,238,19);  // samtools sort
            		case 6: return new Color(96,74,123);   // samtools merge
            		case 7: return new Color(96,74,123);   // samtools merge
            		case 8: return new Color(149,55,53);   // pileup
            		case 9: return new Color(49,133,156);  // varscan
            		case 10: return new Color(20,137,89);  // diff
//            		case 6: return new Color(96,74,123);
//            		case 7: return new Color(96,74,123);
            		default: return new Color(127,127,127); // stage in / out
            	}
            }
        };
        Transformer<Task,Shape> vertexSize = new Transformer<Task,Shape>(){
            public Shape transform(Task task){
                Ellipse2D circle = new Ellipse2D.Double(-15, -15, 30, 30);
                if (breadth[task.getDepth()] > 25) {
                	circle = new Ellipse2D.Double(-5, -15, 10, 30);
                }
                if (breadth[task.getDepth()] > 250) {
                	circle = new Ellipse2D.Double(-2, -15, 4, 30);
                }
                return circle;
            }
        };
        Transformer<Task,Stroke> vertexStroke = new Transformer<Task,Stroke>(){
            public Stroke transform(Task i){
                return new BasicStroke(0f);
            }
        };
		
		// to file
		VisualizationImageServer<Task, DataDependency> vis = new VisualizationImageServer<Task, DataDependency>(vv.getGraphLayout(), vv.getGraphLayout().getSize());
		vis.setPreferredSize(size);
		vis.getRenderContext().setVertexFillPaintTransformer(vertexColor);
		vis.getRenderContext().setVertexShapeTransformer(vertexSize);
		vis.getRenderContext().setVertexStrokeTransformer(vertexStroke);
//		vis.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<Task>());
//		vis.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<DataDependency>());
//		vis.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);
		BufferedImage image = (BufferedImage) vis.getImage(new Point2D.Double(vv.getGraphLayout().getSize().getWidth() / 2,
			    vv.getGraphLayout().getSize().getHeight() / 2), new Dimension(vv.getGraphLayout().getSize()));
		try {
			ImageIO.write(image, "png", new java.io.File("test.png"));
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	/*public void setDeadline(double compute, double bw){
		double lcp=Double.MIN_VALUE;
		Map <Integer, Double> cp=new HashMap<>();
		double temp=0.0;
		List<Task>sortedTasks=new ArrayList<>(getTasks());
		Collections.sort(sortedTasks);
		for (int i = sortedTasks.size() - 1; i >= 0; i--) {
			Task t = sortedTasks.get(i);
		
	//	for(Task t:getSortedTasks()){
			if(!cp.containsKey(t.getCloudletId()))
				cp.put(t.getCloudletId(),t.getCloudletOutputSize()/bw+t.getCloudletLength()/compute);
			for (DataDependency outgoingEdge : t.getWorkflow().getGraph().getOutEdges(t)) {
				Task child = t.getWorkflow().getGraph().getDest(outgoingEdge);
				temp=cp.get(t.getCloudletId())+child.getCloudletOutputSize()/bw+child.getCloudletLength()/compute;
				if(!cp.containsKey(child.getCloudletId()))
					cp.put(child.getCloudletId(), temp);	
				else if(temp>cp.get(child.getCloudletId()))
					cp.replace(child.getCloudletId(), temp);
				if(lcp<temp)
					lcp=temp;
			}
		}
		Random rand=new Random();
		//double dl=lcp* (rand.nextInt(17)+3);
		double dl=lcp+getSubmitTime();	
		this.deadline = dl;
Log.printLine(deadline);
		setSubDLTask(compute, bw);
	}*/
	public void setDeadline(double deadline){
		Random rand=new Random(Parameters.seed++);
		this.deadline=deadline*(rand.nextInt(17)+3)+submitTime;
		//this.deadline=deadline+submitTime;
	}
	public void computeDeadline(double compute, double bw){
		double deadline=0.0;
		Map<Task,Double> EFT=new HashMap();
		for (Task task:getSortedTasks()) {
			double maxest = 0;
						
			for (DataDependency ingoingEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(ingoingEdge);				
				double transferTime=task.getInputFilelist().get(parent).getSize()/Parameters.OUTBW;
				//Log.printLine(task.getCloudletId()+"~~~~~"+task.getInputFilelist().get(parent).getSize()+
				//		"~~~~~"+task.getName()+"~~~~~~~"+bw+"~~~~~~"+transferTime+"~~~~");
				if (maxest<EFT.get(parent)+transferTime) 
					maxest = EFT.get(parent)+transferTime;
				}
			EFT.put(task, maxest+task.getCloudletLength()/compute);
			task.setEst(maxest);
			if(deadline<maxest+task.getCloudletLength()/compute)
				deadline=maxest+task.getCloudletLength()/compute;
			//Log.printLine(task.getCloudletId()+"------"+task.getCloudletLength()+"---"+compute+"----"+(task.getCloudletLength()/compute)+"---"+maxest);
		}
		//	

			this.deadline=deadline;
			Log.printLine(this.deadline);
	}
	public void setLST(double compute){
		Map<Task,Double> LST=new HashMap();
		for (int i = getSortedTasks().size() - 1; i >= 0; i--) {
			Task task = getSortedTasks().get(i);
			double minlft = deadline;
				
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
				double transferTime=task.getOutputFilelist().get(child).getSize()/Parameters.OUTBW;
				if (minlft>LST.get(child)-transferTime) 
					minlft = LST.get(child)-transferTime;
				
//Log.printLine(task.getCloudletId()+"---"+task.getOutputFilelist().get(child).getSizeInByte()+"----"+child.getCloudletId());				
				}
			LST.put(task, minlft-task.getCloudletLength()/compute);
			task.setLst(minlft-task.getCloudletLength()/compute);
			}
		/*for(Task task:sortedTasks){
		Log.printLine(task.getCloudletId()+"-eft-"+EFT.get(task)+"--est-"+task.getEst());
		Log.printLine(task.getCloudletId()+"-lft-"+LST.get(task)+"--lst-"+task.getLst());
		if(task.getEst()==task.getLst())
			Log.printLine(task.getCloudletId()+"------------");}		*/	
	}
	public double getDeadline() {
		return deadline;
	}

	public double getSubmitTime() {
		return submitTime;
	}

	public void setSubmitTime(double submitTime) {
		for(Task t:getTasks())
			t.setSubmitTime(submitTime);
		this.submitTime = submitTime;
	}
	public double getFinishTime() {
		double time=0.0;
		for(Task t:getSortedTasks())
			if(time<t.getFinishTime())
				time=t.getFinishTime();
		return time;
	}
	
	
	public void setPlist(double compute, double bw){
		List<Task> tasks=getSortedTasks();
		List<Partition> list=bipartition(tasks);
		List<Partition> list2=new ArrayList<>();
		int size=getTasks().size();
		while(size>=Parameters.PartitionSize){
			for(Partition p:list)
				list2.addAll(bipartition(p.getSortedTasks()));
				
			list.clear();
			list.addAll(list2);
			list2.clear();
			size=list.get(0).getTasks().size();
		}
		/*while(list.size()>0){
			for(Partition p:list)
				if(p.getTasks().size()>100)
					list2.addAll(bipartition(p.getSortedTasks()));
				else
					multipartition(p.getSortedTasks());	
			list.clear();
			list.addAll(list2);
			list2.clear();
		}*/
		this.plist=list;
		for(Partition p:plist){
		for(Task tta:p.getTasks())
		Log.print(tta.getCloudletId()+"-");	
		Log.printLine("///"+p.getTasks().size()+"----"+p.outerCut()+"===list=="+p.innerCut());
		}
		//for(Partition p:plist)
		//	p.setWeight(compute, bw);
		
	}
	
	private void multipartition(List<Task> tasks){
		
		while(tasks.size()>=2){
			ArrayList<Task> ta=new ArrayList<>();
			ta.add(tasks.get(0));
			Partition pa=new Partition(ta);
			tasks.removeAll(ta);
			ArrayList<Task> tb=new ArrayList<>();
			
			tb.add(tasks.get(tasks.size()-1));
			Partition pb=new Partition(tb);
			tasks.removeAll(tb);
			Map <Task,Long> cuts=new HashMap<>();
			class CutComparator implements Comparator<Task> {
					@Override
					public int compare(Task task1, Task task2) {
						return Long.compare(cuts.get(task1), cuts.get(task2));
					}			
				}
			int token=0;
			while(tasks.size()>0){
				List<Task> candidate=new ArrayList<>();
				int aimindex=0;

				if(token==0){
					for(Task task:tasks)
						cuts.put(task, pb.taskCut(task));
					
					Collections.sort(tasks,new CutComparator());
			
					long min1=cuts.get(tasks.get(0));
					for(Task task:tasks)
						if(cuts.get(task)==min1)
							candidate.add(task);
				
					long max=0;
					for(Task task:candidate){
						long cut=pa.taskCut(task);
						if(max<=cut){
							max=cut;
							aimindex=candidate.indexOf(task);
						}
					}
					pa.addTask(candidate.get(aimindex));
					token=1;
					tasks.remove(candidate.get(aimindex));
					cuts.clear();
					candidate.clear();
				}
				if(token==1&&tasks.size()>0){
					for(Task task:tasks)
						cuts.put(task, pa.taskCut(task));
					Collections.sort(tasks,new CutComparator());
			
					long min1=cuts.get(tasks.get(0));
					for(Task task:tasks)
						if(cuts.get(task)==min1)
							candidate.add(task);
				
					long max=0;
					for(Task task:candidate){
						long cut=pb.taskCut(task);
						if(max<=cut){
							max=cut;
							aimindex=candidate.indexOf(task);
						}
					}
					pb.addTask(candidate.get(aimindex));

					token=0;
					tasks.remove(candidate.get(aimindex));
					cuts.clear();
					candidate.clear();
				}			
			}//for(Task tta:pa.getTasks())Log.print(tta.getCloudletId()+"-");	Log.printLine();
			//for(Task tta:pb.getTasks())Log.print(tta.getCloudletId()+"-");	Log.printLine();
			tasks.clear();
			if(pa.innerCut()>=pb.innerCut()){
				plist.add(pa);
				tasks.addAll(pb.getSortedTasks());
			}
			else{
				plist.add(pb);
				tasks.addAll(pa.getSortedTasks());
			}
		}
		if(tasks.size()==1){
			Partition p1=new Partition(tasks);
			plist.add(p1);
		}
			
/*for(Partition p:plist){
for(Task tta:p.getTasks())
Log.print(tta.getCloudletId()+"-");	
Log.printLine("/////"+p.getTasks().size()+"----"+p.outerCut()+"==multi==="+p.innerCut());
}*/
	}
	protected List<Partition> bipartition(List<Task> tasks){
		List<Partition> list=new ArrayList<>();
		ArrayList<Task> ta=new ArrayList<>();
		ta.add(tasks.get(0));
		Partition pa=new Partition(ta);
		if(tasks.size()==1){
			list.add(pa);
			return list;
		}
		
		tasks.removeAll(ta);
		
		ArrayList<Task> tb=new ArrayList<>();
				
		tb.add(tasks.get(tasks.size()-1));
		Partition pb=new Partition(tb);
		if(tasks.size()==1){
			list.add(pb);
			return list;
		}
		tasks.removeAll(tb);

		Map <Task,Long> cuts=new HashMap<>();
			class CutComparator implements Comparator<Task> {
			@Override
				public int compare(Task task1, Task task2) {
							return Long.compare(cuts.get(task1), cuts.get(task2));
				}			
			}
		int token=0;
		while(tasks.size()>0){
			List<Task> candidate=new ArrayList<>();
			int aimindex=0;

			if(token==0){
				for(Task task:tasks)
					cuts.put(task, pb.taskCut(task));
						
				Collections.sort(tasks,new CutComparator());
				
				long min1=cuts.get(tasks.get(0));
				for(Task task:tasks)
					if(cuts.get(task)==min1)
						candidate.add(task);
					
				long max=0;
				for(Task task:candidate){
				long cut=pa.taskCut(task);
				if(max<=cut){
					max=cut;
					aimindex=candidate.indexOf(task);
					}
				}
				pa.addTask(candidate.get(aimindex));//Log.print(candidate.get(aimindex).getCloudletId()+"-a-remove-");
				token=1;
				tasks.remove(candidate.get(aimindex));
				cuts.clear();
				candidate.clear();
			}
			if(token==1&&tasks.size()>0){
				for(Task task:tasks)
					cuts.put(task, pa.taskCut(task));
				Collections.sort(tasks,new CutComparator());
				
				long min1=cuts.get(tasks.get(0));
				for(Task task:tasks)
					if(cuts.get(task)==min1)
						candidate.add(task);
					
				long max=0;
				for(Task task:candidate){
					long cut=pb.taskCut(task);
					if(max<=cut){
						max=cut;
						aimindex=candidate.indexOf(task);
					}
				}
				pb.addTask(candidate.get(aimindex));//Log.print(candidate.get(aimindex).getCloudletId()+"-b-remove-");

				token=0;
				tasks.remove(candidate.get(aimindex));
				cuts.clear();
				candidate.clear();
			}			
		}				
		list.add(pa);
		list.add(pb);
		/*for(Partition p:list){
			for(Task tta:p.getTasks())
			Log.print(tta.getCloudletId()+"-");	
			Log.printLine("///"+p.getTasks().size()+"----"+p.outerCut()+"===bi=="+p.innerCut());
			}*/
		return list;
	}
	public List<Partition> getPlist() {
		return plist;
	}
	public List<Partition> getExcPlist() {
		List <Partition> execPlist=new ArrayList<>();
		for(Partition p:plist){
			List<Task> exec=new ArrayList<Task>();
			for(Task t:p.getTasks())
				if(t.getStatus()!=0)
					exec.add(t);
			p.getTasks().removeAll(exec);
			if(p.getTasks().size()>0)
				execPlist.add(p);
		}
		return execPlist;
	}
	public List<Partition> getBeforeExcPlist(Task task) {
		List <Partition> execPlist=new ArrayList<>();
		for(Partition p:plist){
			List<Task> exec=new ArrayList<Task>();
			if(p.before(task)||p.containTask(task)){
				for(Task t:p.getTasks())
					if(t.getStatus()!=0)
						exec.add(t);
				p.getTasks().removeAll(exec);
				if(p.getTasks().size()>0)
					execPlist.add(p);
			}
		}
		return execPlist;
	}

	public List<Task> getSortedTask() {
		return sortedTask;
	}

	public void setSortedTask(List<Task> sortedTask) {
		this.sortedTask = sortedTask;
	}
}
