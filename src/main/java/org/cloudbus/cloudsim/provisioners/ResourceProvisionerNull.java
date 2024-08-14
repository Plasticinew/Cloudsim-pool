package org.cloudbus.cloudsim.provisioners;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.resources.Resource;
import org.cloudbus.cloudsim.resources.ResourceManageable;
import org.cloudbus.cloudsim.vms.Vm;

/**
 * A class that implements the Null Object Design Pattern for
 * {@link ResourceProvisioner} class.
 *
 * @author Manoel Campos da Silva Filho
 * @see ResourceProvisioner#NULL
 */
class ResourceProvisionerNull implements ResourceProvisioner {
    @Override public boolean allocateResourceForVm(Vm vm, long newTotalVmResourceCapacity) {
        return false;
    }
    @Override public long getAllocatedResourceForVm(Vm vm) {
        return 0;
    }
    @Override public long getTotalAllocatedResource() {
        return 0;
    }
    @Override public void setRelatedHost(List<Host> relatedHosts) {
    }

    @Override public List<Host> getRelatedHost() {
        List<Host> zero = new ArrayList<>();
        return zero;
    }
    @Override public long deallocateResourceForVm(Vm vm) {
        return 0;
    }
    @Override public boolean isSuitableForVm(Vm vm, long newVmTotalAllocatedResource) {
        return false;
    }
    @Override public boolean isSuitableForVm(Vm vm, Resource resource) { return false; }
    @Override public ResourceManageable getPmResource() {
        return ResourceManageable.NULL;
    }
    @Override public void setResources(ResourceManageable pmResource, Function<Vm, ResourceManageable> vmResourceFunction) {/**/}
    @Override public long getCapacity() { return 0; }
    @Override public long getAvailableResource() { return 0; }
}
