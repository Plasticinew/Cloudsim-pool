import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Random;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAR2;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAR3;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerBestFit;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisioner;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Bandwidth;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.resources.Ram;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.util.Log;

import ch.qos.logback.classic.Level;

/**
 * Simulation of Azure VM traces using CloudSim Plus.
 * The code in based on the official example:
 * https://github.com/cloudsimplus/cloudsimplus-examples/blob/master/src/main/java/org/cloudsimplus/examples/power/PowerExample.java
 *
**/
public class AzureVmTraceExample {
    // Defines, between other things, the time intervals to keep Hosts CPU utilization history records
    private static final int SCHEDULING_INTERVAL = 60;
    // MIPS performance of PE
    private static final int PE_MIPS = 1000;
    // CPUs per host
    private static final int HOST_PES = 256;
    // Host memory capacity in GB
    private static final int HOST_MEMORY = 1024;
    // Host nic bandwidth in Gbps
    private static final int HOST_BW = 400;
    // Indicates the time (in seconds) the Host takes to start up
    private static final double HOST_START_UP_DELAY = 0;
    // Indicates the time (in seconds) the Host takes to shut down
    private static final double HOST_SHUT_DOWN_DELAY = 3;
    // Indicates Host power consumption (in Watts) during startup
    private static final double HOST_START_UP_POWER = 5;
    // Indicates Host power consumption (in Watts) during shutdown
    private static final double HOST_SHUT_DOWN_POWER = 3;
    // Defines the power a Host uses, even if it's idle (in Watts)
    private static final double STATIC_POWER = 35;
    // The max power a Host uses (in Watts)
    private static final int MAX_POWER = 50;
    // Just comma (,) symbol
    private static final String COMMA_DELIMITER = ",";

    private static final int HOST_PER_RACK = 8;
    private static final int DPU_PER_RACK = 4;

    private static final double CPU_WEIGHT = 1;
    private static final double MEM_WEIGHT = 1;
    private static final double DPU_WEIGHT = 1;

    List<AzureVmType> vmTypes;
    List<Integer> weights;
    public static void main(String[] args) throws Exception {
        new AzureVmTraceExample(args[0], args[1], Double.parseDouble(args[2]), Integer.parseInt(args[3]));
    }

    public double valueFunction(AzureVmType type){
        return type.cpu * CPU_WEIGHT + type.memory * MEM_WEIGHT + type.bw * DPU_WEIGHT;
        // return weights.get(type.vmTypeId);
    }

    public double pointFunctionAR2(Point2D p){
        double sum_point = 0;
        for(int i = 0;i < vmTypes.size(); i++){
            if(weights.get(i) > 0){
                var point = (double)Math.min((int)(p.getX()/vmTypes.get(i).cpu), (int)(p.getY()/vmTypes.get(i).memory)) * vmTypes.get(i).cpu;
                sum_point += point;
            }
        }
        // System.out.printf("find result %f\n", sum_point);
        return sum_point;
    }

    public double pointFunctionAR3Node(Point2D p, double dpu){
        double sum_point = 0;
        for(int i = 0;i < vmTypes.size(); i++){
            if(weights.get(i) > 0){
                var point = (double)Math.min((int)(p.getX()/vmTypes.get(i).cpu), (int)(p.getY()/vmTypes.get(i).memory)) 
                                * valueFunction(vmTypes.get(i));
                sum_point += point;
            }
        }
        // System.out.printf("find result %f\n", sum_point);
        return sum_point;
    }

    public double pointFunctionAR3DPU(Point2D p, double dpu){
        double sum_point = 0;
        for(int i = 0;i < vmTypes.size(); i++){
            if(weights.get(i) > 0){
                var point = (double)(int)(dpu/vmTypes.get(i).bw) 
                                * valueFunction(vmTypes.get(i));
                sum_point += point;
            }
        }
        // System.out.printf("find result %f\n", sum_point);
        return sum_point;
    }

    public double pointFunctionAR3(List<Point2D> point, List<Double> dpulist){
        double sum_point = 0;
        for(int i = 0;i < vmTypes.size(); i++){
            if(weights.get(i) > 0){
                int node_sum = 0, dpu_sum = 0;
                for(var p : point){
                    node_sum += Math.min((int)(p.getX()/vmTypes.get(i).cpu),(int)(p.getY()/vmTypes.get(i).memory));
                }

                for(var dpu : dpulist){
                    dpu_sum += (int)(dpu/vmTypes.get(i).bw);
                }
                sum_point += (double)Math.min(dpu_sum, node_sum) * 
                    valueFunction(vmTypes.get(i));
                    // (vmTypes.get(i).cpu * CPU_WEIGHT + vmTypes.get(i).memory * MEM_WEIGHT + vmTypes.get(i).bw * DPU_WEIGHT);
            }
        }
        // System.out.printf("find result %f\n", sum_point);
        return sum_point;
    }

    private AzureVmTraceExample(String vmTypesPath, String vmInstancesPath,
                                double simulationTime, int host_count) throws Exception {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(Level.ERROR);
        //Log.setLevel(DatacenterBroker.LOGGER, Level.ERROR);

        final CloudSim simulation = new CloudSim();
        final var hosts = createHosts(host_count);
        System.out.printf("time: %f, host: %d, dpu: %d\n", simulationTime, host_count, DPU_PER_RACK);
        var policy = new VmAllocationPolicyAR2();
        policy.setPointFunction(point -> this.pointFunctionAR2(point));
        System.out.printf("using ar2\n");
        // var policy = new VmAllocationPolicyAR3();
        // policy.setPointFunction((point, list) -> this.pointFunctionAR3(point, list));
        // policy.setPointFunction((point, dpu) -> this.pointFunctionAR3Node(point, dpu), (point, dpu) -> this.pointFunctionAR3DPU(point, dpu));
        // // System.out.printf("using ar3\n");
        // System.out.printf("using ar3 weight\n");
        final Datacenter dc = new DatacenterSimple(simulation, hosts, policy);
        // final Datacenter dc = new DatacenterSimple(simulation, hosts, new VmAllocationPolicyBestFit());
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        // Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs
        DatacenterBroker broker = new DatacenterBrokerBestFit(simulation);
        broker.setVmDestructionDelay(1);
        weights = new ArrayList<>(Collections.nCopies(500, -1));
        vmTypes = readVmTypes(vmTypesPath);
        var vmInstances = readVmInstances(vmInstancesPath);

        List<Vm> vmList = new ArrayList<>();
        List<Cloudlet> cloudletList = new ArrayList<>();
        int id = 0;
        for (AzureVmInstance instance: vmInstances) {
            if (instance.startTime < 0) {
                continue;
            }
            if (instance.startTime > simulationTime) {
                break;
            }
            var finishTime = simulationTime;
            if (instance.endTime.isPresent() && instance.endTime.getAsDouble() < simulationTime) {
                finishTime = instance.endTime.getAsDouble();
            }

            final var duration = finishTime - instance.startTime;
            if((long)(duration) == 0) {
                continue;
            }
            final var vmId = id++;
            var vmType = vmTypes.get(instance.vmTypeId);
            final var vmPes = Math.max((int)(vmType.cpu * HOST_PES), 1);
            final var vmRam = Math.max((int)(vmType.memory * HOST_MEMORY * 1024), 1);
            final var vmBw = Math.max((int)(vmType.bw * HOST_BW * 1024), 1);
            // final var vmPes = Math.max((int)(vmType.cpu), 1);
            // final var vmRam = Math.max((int)(vmType.memory* 1024), 1);
            // final var vmBw = Math.max((int)(vmType.bw* 1024), 1);
            final var vmSize = 1000;
            final var vm = new VmSimple(vmId, PE_MIPS, vmPes);
            vm.setRam(vmRam).setBw(vmBw).setSize(vmSize).enableUtilizationStats();
            vm.setSubmissionDelay(instance.startTime);
            vm.setStopTime(finishTime);
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());
            vm.enableUtilizationStats();
            vmList.add(vm);

            final var cloudlet = createCloudlet(vmId, vm, vmPes, duration);
            cloudlet.setExecStartTime(instance.startTime);
            cloudletList.add(cloudlet);
        }
        System.out.println("Number of VMs is " + vmList.size());

        long timeStart = System.currentTimeMillis();

        broker.submitVmList(vmList);
        broker.submitCloudletList(cloudletList);
        // simulation.addOnClockTickListener(Null -> printHostCpuUtilizationAndPowerConsumption(hosts));
        // simulation.addOnEventProcessingListener(Null -> printHostCpuUtilizationAndPowerConsumption(hosts));
        simulation.start();

        var timeFinish = System.currentTimeMillis();
        var timeElapsed = timeFinish - timeStart;
        System.out.println("Elapsed time is " + timeElapsed / 1000.0 + " seconds");
        printHostCpuUtilizationAndPowerConsumption(hosts);
        // final var cloudletFinishedList = broker.getCloudletFinishedList();
        // new CloudletsTableBuilder(cloudletFinishedList).build();
    }

    // CloudSim --------------------------------------------------------------------------------------------------------

    private List<Host> createHosts(int host_count) {
        final List<Host> hostList = new ArrayList<>(host_count);
        for(int i = 0; i < host_count; i++) {
            final var host = createRack(i*HOST_PER_RACK);
            hostList.addAll(host);
        }
        return hostList;
    }

    private List<Host> createRack(final int id) {
        final List<Host> RackHosts = new ArrayList<>(HOST_PER_RACK);
        
        final List<ResourceProvisioner> bwProvisioner_ = new ArrayList<>(DPU_PER_RACK);
        for(int i = 0; i < DPU_PER_RACK; i++) {
            final var bw_ = new Bandwidth(HOST_BW * 1024); //in Megabits/s
            var bwp = new ResourceProvisionerSimple();
            bwp.setResources(bw_, vm -> ((VmSimple)vm).getBw());
            bwProvisioner_.add(bwp);
        }

        for(int i = 0; i < HOST_PER_RACK; i++) {
            
            // final List<ResourceProvisioner> bwProvisioner_ = new ArrayList<>();
            // final var bw_ = new Bandwidth(HOST_BW * 1024); //in Megabits/s
            // var bwp = new ResourceProvisionerSimple();
            // bwp.setResources(bw_, vm -> ((VmSimple)vm).getBw());
            // bwProvisioner_.add(bwp);

            final var peList = new ArrayList<Pe>(HOST_PES);
            //List of Host's CPUs (Processing Elements, PEs)
            for (int j = 0; j < HOST_PES; j++) {
                peList.add(new PeSimple(PE_MIPS));
            }
            ResourceProvisioner ramProvisioner_ = new ResourceProvisionerSimple();
            final var ram_ = new Ram(HOST_MEMORY * 1024); //in Megabytes
            ramProvisioner_.setResources(ram_, vm -> ((VmSimple)vm).getRam());
            final long storage = 1000000; //in Megabytes
            final var vmScheduler = new VmSchedulerSpaceShared();

            final var host = new HostSimple(ramProvisioner_, bwProvisioner_, storage, peList);

            final var powerModel = new PowerModelHostSimple(MAX_POWER, STATIC_POWER);
            powerModel.setStartupDelay(HOST_START_UP_DELAY)
                    .setShutDownDelay(HOST_SHUT_DOWN_DELAY)
                    .setStartupPower(HOST_START_UP_POWER)
                    .setShutDownPower(HOST_SHUT_DOWN_POWER);

            host.setVmScheduler(vmScheduler).setPowerModel(powerModel);
            host.setId(id+i);
            host.enableUtilizationStats();
            RackHosts.add(host);
        }
        for(int i = 0; i < DPU_PER_RACK; i++) {
            bwProvisioner_.get(i).setRelatedHost(RackHosts);
        }
        for(var host:RackHosts){
            host.setRelatedHost(RackHosts);
        }
        return RackHosts;
    }

    private Cloudlet createCloudlet(final int id, final Vm vm, final int vmPes, final double duration) {
        final long fileSize = 1;
        final long outputSize = 1;
        final long length = (long) (duration * PE_MIPS); // in number of Million Instructions (MI)
        final var utilizationModel = new UtilizationModelFull();

        return new CloudletSimple(id, length, vmPes)
            .setFileSize(fileSize)
            .setOutputSize(outputSize)
            .setUtilizationModel(utilizationModel)
            .setVm(vm);
    }

    private void printHostCpuUtilizationAndPowerConsumption(final List<Host> hosts) {
        double accumulatedCPUUtilization = 0;
        double accumulatedBWUtilization = 0;
        var capacity = hosts.get(0).getTotalMipsCapacity();
        double accumulatedRAMUtilization = 0;
        for (Host host: hosts) {
            final double utilizationPercentMean = host.getTotalAllocatedMips()/capacity;
            final double nicUilization = host.getBwUtilization();
            final double ramUilization = host.getRamUtilization();
            
            // The total Host's CPU utilization for the time specified by the map key
            // final double utilizationPercentMean = cpuStats.getMean();
            accumulatedCPUUtilization += utilizationPercentMean * 100;
            accumulatedBWUtilization += nicUilization / DPU_PER_RACK / (HOST_BW*1024.0) * 100.0 ;
            accumulatedRAMUtilization += ramUilization / (HOST_MEMORY*1024.0) * 100.0 ;
            // System.out.printf("%.1f%% ", 
            //     nicUilization / DPU_PER_RACK / (HOST_BW*1024.0) * 100.0 );    
        }
        // System.out.println();
        System.out.printf("%.1f%%, ", 
            accumulatedCPUUtilization / hosts.size());
        // System.out.println();
        System.out.printf("%.1f%%, ", 
            accumulatedBWUtilization / hosts.size());
        System.out.printf("%.1f%%", 
            accumulatedRAMUtilization / hosts.size());
        System.out.println();
    }

    // Dataset ---------------------------------------------------------------------------------------------------------

    private static class AzureVmType {
        String id;
        int vmTypeId;
        double cpu;
        double memory;
        double bw;

        public AzureVmType(String[] values) {
            this.id = values[0];
            this.vmTypeId = Integer.parseInt(values[1]);
            this.cpu = Math.min(Double.parseDouble(values[2])*0.99, 1);
            this.memory = Math.min(Double.parseDouble(values[3])*1.1, 1);
            this.bw = Math.min(Double.parseDouble(values[4])*0.7, 1);
            // this.cpu = Double.parseDouble(values[2]);
            // this.memory = Double.parseDouble(values[3]);
            // this.bw = Double.parseDouble(values[4]);
        }
    }

    private static class AzureVmInstance {
        int vmId;
        int vmTypeId;
        double startTime;
        OptionalDouble endTime;

        public AzureVmInstance(String[] values) {
            this.vmId = Integer.parseInt(values[0]);
            this.vmTypeId = Integer.parseInt(values[1]);
            this.startTime = Double.parseDouble(values[2]) * 86400;
            if (!values[3].equals("none") && !values[3].equals("")) {
                this.endTime = OptionalDouble.of(Double.parseDouble(values[3]) * 86400) ;
            } else {
                this.endTime = OptionalDouble.empty();
            }
        }
    }

    private ArrayList<AzureVmType> readVmTypes(String vmTypesPath) throws Exception {
        ArrayList<AzureVmType> records = new ArrayList<>(Collections.nCopies(500, null));
        try (BufferedReader br = new BufferedReader(new FileReader(vmTypesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                }
                String[] values = line.split(COMMA_DELIMITER);
                AzureVmType type = new AzureVmType(values);
                var typeid = Integer.parseInt(values[1]);
                if(records.get(typeid) != null) {
                    var record = records.get(type.vmTypeId);
                    var max_val = Math.max(Math.max(record.bw, record.cpu), record.memory);
                    if((Math.min(Double.parseDouble(values[4])*0.7, 1) >= record.bw && max_val == record.bw) || 
                            // (Math.min(Double.parseDouble(values[2]), 1) >= record.cpu && max_val == record.cpu) || 
                            (Math.min(Double.parseDouble(values[3])*1.1, 1) >= record.memory && max_val == record.memory) ){
                        records.set(type.vmTypeId, type);
                    }
                } else {
                    weights.set(type.vmTypeId, 0);
                    records.set(type.vmTypeId, type);
                }
            }
        }
        return records;
    }

    private ArrayList<AzureVmInstance> readVmInstances(String vmInstancesPath) throws Exception {
        ArrayList<AzureVmInstance> records = new ArrayList<>();
        var rand = new Random(100);
        try (BufferedReader br = new BufferedReader(new FileReader(vmInstancesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                }
                line += ", none";
                String[] values = line.split(COMMA_DELIMITER);
                var id = Integer.parseInt(values[0])*1;
                var typeid = Integer.parseInt(values[1])%91;
                for(int i = 0; i < 1; i++) {
                    values[0] = Integer.toString(id+i);
                    values[1] = Integer.toString(rand.nextInt(27));
                    AzureVmInstance instance = new AzureVmInstance(values);
                    records.add(instance);
                    weights.set(instance.vmTypeId, weights.get(instance.vmTypeId)+1);
                }
            }
        }
        return records;
    }
}
