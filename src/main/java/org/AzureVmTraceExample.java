import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.OptionalDouble;
import java.awt.geom.Point2D;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAR2;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAR3;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyBestFit;
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
import org.cloudbus.cloudsim.vms.HostResourceStats;
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
    private static final int HOST_PES = 128;
    // Host memory capacity in GB
    private static final int HOST_MEMORY = 256;
    // Host nic bandwidth in Gbps
    private static final int HOST_BW = 100;
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

    HashMap<String, AzureVmType> vmTypes;

    public static void main(String[] args) throws Exception {
        new AzureVmTraceExample(args[0], args[1], Double.parseDouble(args[2]), Integer.parseInt(args[3]));
    }

    public double pointFunctionAR2(Point2D p){
        double sum_point = 0;
        for(var type: vmTypes.entrySet()){
            var point = (double)Math.min((int)(p.getX()/type.getValue().cpu), (int)(p.getY()/type.getValue().memory)) * type.getValue().cpu;
            sum_point += point;
        }
        // System.out.printf("find result %f\n", sum_point);
        return sum_point;
    }

    public double pointFunctionAR3Node(Point2D p){
        double sum_point = 0;
        for(var type: vmTypes.entrySet()){
            var point = (double)Math.min((int)(p.getX()/type.getValue().cpu), (int)(p.getY()/type.getValue().memory)) 
                            * (type.getValue().cpu * CPU_WEIGHT + type.getValue().memory * MEM_WEIGHT);
            sum_point += point;
        }
        // System.out.printf("find result %f\n", sum_point);
        return sum_point;
    }

    public double pointFunctionAR3DPU(double p){
        double sum_point = 0;
        for(var type: vmTypes.entrySet()){
            var point = (double)(int)(p/type.getValue().bw) * type.getValue().bw * DPU_WEIGHT;
            sum_point += point;
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
        var policy = new VmAllocationPolicyAR2();
        policy.setPointFunction(point -> this.pointFunctionAR2(point));
        // var policy = new VmAllocationPolicyAR3();
        // policy.setPointFunction(point -> this.pointFunctionAR3Node(point), dpu -> this.pointFunctionAR3DPU(dpu));
        final Datacenter dc = new DatacenterSimple(simulation, hosts, policy);
        // final Datacenter dc = new DatacenterSimple(simulation, hosts, new VmAllocationPolicyBestFit());
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        // Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs
        DatacenterBroker broker = new DatacenterBrokerBestFit(simulation);
        broker.setVmDestructionDelay(1);

        vmTypes = readVmTypes(vmTypesPath);
        var vmInstances = readVmInstances(vmInstancesPath);

        List<Vm> vmList = new ArrayList<>();
        List<Cloudlet> cloudletList = new ArrayList<>();
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
            final var vmId = instance.vmId;
            var vmType = vmTypes.get(instance.vmTypeId);
            final var vmPes = Math.max((int)(vmType.cpu * HOST_PES), 1);
            final var vmRam = Math.max((int)(vmType.memory * HOST_MEMORY * 1024), 1);
            final var vmBw = Math.max((int)(vmType.bw * HOST_BW * 1024), 1);
            final var vmSize = 1000;
            final var vm = new VmSimple(instance.vmId, PE_MIPS, vmPes);
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
        simulation.addOnClockTickListener(Null -> printHostCpuUtilizationAndPowerConsumption(hosts));
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
        double accumulatedRAMUtilization = 0;
        for (Host host: hosts) {
            final double utilizationPercentMean = host.getBusyPesPercent();
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
        String vmTypeId;
        double cpu;
        double memory;
        double bw;

        public AzureVmType(String[] values) {
            this.id = values[0];
            this.vmTypeId = values[1];
            this.cpu = Math.min(Double.parseDouble(values[2]), 1);
            this.memory = Math.min(Double.parseDouble(values[3]), 1);
            this.bw = Math.min(Double.parseDouble(values[4]), 1);
        }
    }

    private static class AzureVmInstance {
        int vmId;
        String vmTypeId;
        double startTime;
        OptionalDouble endTime;

        public AzureVmInstance(String[] values) {
            this.vmId = Integer.parseInt(values[0]);
            this.vmTypeId = values[1];
            this.startTime = Double.parseDouble(values[2]) * 86400;
            if (!values[3].equals("none") && !values[3].equals("")) {
                this.endTime = OptionalDouble.of(Double.parseDouble(values[3]) * 86400) ;
            } else {
                this.endTime = OptionalDouble.empty();
            }
        }
    }

    private HashMap<String, AzureVmType> readVmTypes(String vmTypesPath) throws Exception {
        HashMap<String, AzureVmType> records = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(vmTypesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                }
                String[] values = line.split(COMMA_DELIMITER);
                AzureVmType type = new AzureVmType(values);
                // if(records.containsKey(type.vmTypeId)) {
                //     if(Double.parseDouble(values[4]) < records.get(type.vmTypeId).bw && Double.parseDouble(values[2]) < records.get(type.vmTypeId).cpu ){
                //         records.put(type.vmTypeId, type);
                //     }
                // } else {
                    records.put(type.vmTypeId, type);
                // }
            }
        }
        return records;
    }

    private ArrayList<AzureVmInstance> readVmInstances(String vmInstancesPath) throws Exception {
        ArrayList<AzureVmInstance> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(vmInstancesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                }
                line += ", none";
                String[] values = line.split(COMMA_DELIMITER);
                var id = Integer.parseInt(values[0]) * 10;
                // for(int i = 0; i < 10; i++) {
                    values[0] = Integer.toString(+id);
                    AzureVmInstance instance = new AzureVmInstance(values);
                    records.add(instance);
                // }
            }
        }
        return records;
    }
}
