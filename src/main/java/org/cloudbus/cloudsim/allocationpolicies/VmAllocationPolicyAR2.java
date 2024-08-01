/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2021 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudbus.cloudsim.allocationpolicies;

import java.awt.geom.Point2D;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

/**
 * A Best Fit VmAllocationPolicy implementation that chooses, as
 * the host for a VM, the one with the most number of PEs in use,
 * which has enough free PEs for a VM.
 *
 * <p>This is a really computationally complex policy since the worst-case complexity
 * to allocate a Host for a VM is O(N), where N is the number of Hosts.
 * Such an implementation is not appropriate for large scale scenarios.</p>
 *
 * <p><b>NOTE: This policy doesn't perform optimization of VM allocation by means of VM migration.</b></p>
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 3.0.1
 *
 * @see VmAllocationPolicyFirstFit
 * @see VmAllocationPolicySimple
 */
public class VmAllocationPolicyAR2 extends VmAllocationPolicyAbstract {
    /**
     * Gets the first suitable host from the {@link #getHostList()}
     * that has the highest number of PEs in use (i.e. the least number of free PEs).
     * @return an {@link Optional} containing a suitable Host to place the VM;
     *         or an empty {@link Optional} if not found
     */
    
    Function<Point2D, Double> pointFunction;
    final private HashMap<Point2D, Double> pointRecord = new HashMap<>();

    public void setPointFunction(Function<Point2D, Double> pointFunction){
        this.pointFunction = pointFunction;
    }

    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        /* Since it's being used the min operation, the active comparator must be reversed so that
         * we get active hosts with minimum number of free PEs. */
        var hosts = getHostList();
        double max_delta = Double.NEGATIVE_INFINITY;
        Optional<Host> aim_host = Optional.ofNullable(null);
        for(var host : hosts){
            // find suitable host
            if(host.isSuitableForVm(vm)){
                // first, find suitable dpu
                
                double cpu_before = 1 - host.getBusyPesPercent();
                double mem_before = 1 - host.getRamUtilization() / host.getRamProvisioner().getCapacity();
                double cpu_after = cpu_before - (double)(vm.getExpectedFreePesNumber())/host.getNumberOfPes();
                double mem_after = mem_before - (double)(vm.getRam().getCapacity()) / host.getRamProvisioner().getCapacity();
                var point1 = new Point2D.Double(cpu_before, mem_before);
                var point2 = new Point2D.Double(cpu_after, mem_after);
                double value_before, value_after;
                if(!pointRecord.containsKey(point1)){
                    value_before = pointFunction.apply(point1);
                    pointRecord.put(point1, value_before);
                } else {
                    value_before = pointRecord.get(point1);
                }

                if(!pointRecord.containsKey(point2)){
                    value_after = pointFunction.apply(point2);
                    pointRecord.put(point2, value_after);
                } else {
                    value_after = pointRecord.get(point2);
                }

                var delta_value = value_after - value_before;
                if(delta_value > max_delta) {
                    aim_host = Optional.of(host);
                    max_delta = delta_value;
                }  
            }
        }

        return aim_host;
    }

}