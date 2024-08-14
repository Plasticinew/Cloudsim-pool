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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.commons.collections4.map.MultiKeyMap;
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
public class VmAllocationPolicyAR3 extends VmAllocationPolicyAbstract {
    /**
     * Gets the first suitable host from the {@link #getHostList()}
     * that has the highest number of PEs in use (i.e. the least number of free PEs).
     * @return an {@link Optional} containing a suitable Host to place the VM;
     *         or an empty {@link Optional} if not found
     */
    
    BiFunction<Point2D, Double, Double> pointFunctionNode;
    BiFunction<Point2D, Double, Double> pointFunctionDPU;
    BiFunction<List<Point2D>, List<Double>, Double> pointFunction;
    final private MultiKeyMap<Double, Double> pointRecordNode = new MultiKeyMap<>();
    final private MultiKeyMap<Double, Double> pointRecordDPU = new MultiKeyMap<>();
    // final private HashMap<Point2D, Double> pointRecordNode = new HashMap<>();
    // final private HashMap<Double, Double> pointRecordDPU = new HashMap<>();

    public void setPointFunction(BiFunction<Point2D, Double, Double> pointFunctionNode, BiFunction<Point2D, Double, Double> pointFunctionDPU){
        this.pointFunctionNode = pointFunctionNode;
        this.pointFunctionDPU = pointFunctionDPU;
    }

    public void setPointFunction(BiFunction<List<Point2D>, List<Double>, Double> pointFunction){
        this.pointFunction = pointFunction;
    }

    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        /* Since it's being used the min operation, the active comparator must be reversed so that
         * we get active hosts with minimum number of free PEs. */
        var hosts = getHostList();
        double min_delta = Double.POSITIVE_INFINITY;
        double min_delta_dpu = Double.POSITIVE_INFINITY;
        double min_delta_node = Double.POSITIVE_INFINITY;
        Optional<Host> aim_host = Optional.empty();
        var capacity = hosts.get(0).getTotalMipsCapacity();
        for(var host : hosts){
            // find suitable host
            if(host.isSuitableForVm(vm)){
                var relatedHost = host.getRelatedHost();
                List<Point2D> point = new ArrayList<>();
                List<Double> dpulist = new ArrayList<>();
                int index = 0, j = 0;
                for(var rhost:relatedHost){
                    if(host == rhost){
                        index = j;
                    }
                    j++;
                    double cpu_before = (double)rhost.getTotalAvailableMips()/capacity;
                    double mem_before = (double)rhost.getRamProvisioner().getAvailableResource()/rhost.getRamProvisioner().getCapacity();
                    var point1 = new Point2D.Double(cpu_before, mem_before);
                    point.add(point1);
                } 
                for(var rdpu:host.getBwProvisioner()){
                    double band_before = (double)(rdpu.getAvailableResource())/rdpu.getCapacity();
                    dpulist.add(band_before);
                }
                var init_val = pointFunction.apply(point, dpulist);
                var oldpoint = point.get(index);
                double cpu_after = oldpoint.getX() - (double)(vm.getTotalMipsCapacity())/capacity;
                double mem_after = oldpoint.getY() - (double)(vm.getRam().getCapacity()) / host.getRamProvisioner().getPmResource().getCapacity();
                if(cpu_after < 0) {
                    System.out.printf("cpu error!\n");
                }
                if(mem_after < 0) {
                    System.out.printf("mem error!\n");
                }
                var point2 = new Point2D.Double(cpu_after, mem_after);
                point.set(index, point2);
                for(int i = 0; i < host.getBwProvisioner().size(); i++) {
                    // if(host.getBwProvisioner(i).getAvailableResource() >= vm.getBw().getCapacity()) {
                    var band_before = dpulist.get(i);
                    double band_after = band_before - (double)(vm.getBw().getCapacity())/host.getBwProvisioner(i).getCapacity();
                    if(band_after < 0){
                        continue;
                    }
                    dpulist.set(i, band_after);
                    var delta_value = init_val - pointFunction.apply(point, dpulist);
                    dpulist.set(i, band_before);   
                    if(delta_value < min_delta) {
                        vm.setNicId(i);
                        aim_host = Optional.of(host);
                        min_delta = delta_value;
                        min_delta_node = pointFunctionNode.apply(oldpoint, band_before) - pointFunctionNode.apply(point2, band_after);
                        min_delta_dpu = pointFunctionDPU.apply(oldpoint, band_before) - pointFunctionDPU.apply(point2, band_after);
                    } else if(delta_value == min_delta) {
                        var new_min_delta_node = pointFunctionNode.apply(oldpoint, band_before) - pointFunctionNode.apply(point2, band_after);
                        var new_min_delta_dpu = pointFunctionDPU.apply(oldpoint, band_before) - pointFunctionDPU.apply(point2, band_after);
                        if((new_min_delta_dpu == min_delta_dpu && new_min_delta_node < min_delta_node) || 
                            (new_min_delta_node == min_delta_node && new_min_delta_dpu < min_delta_dpu)) {
                            vm.setNicId(i);
                            aim_host = Optional.of(host);
                            min_delta = delta_value;
                            min_delta_node = new_min_delta_node;
                            min_delta_dpu = new_min_delta_dpu;
                        }
                    }
                    // }
                }
                // // first, find suitable dpu
                // double min_delta_dpu = Double.POSITIVE_INFINITY;
                // double cpu_before = host.getTotalAvailableMips()/capacity;
                // double mem_before = host.getRamProvisioner().getAvailableResource()/host.getRamProvisioner().getCapacity();
                // double cpu_after = cpu_before - (double)(vm.getTotalMipsCapacity())/capacity;
                // double mem_after = mem_before - (double)(vm.getRam().getCapacity()) / host.getRamProvisioner().getCapacity();
                // var point1 = new Point2D.Double(cpu_before, mem_before);
                // var point2 = new Point2D.Double(cpu_after, mem_after);
                // double value_before, value_after;
                // double band_before_final = 0;
                // double band_after_final = 0;

                // for(int i = 0; i < host.getBwProvisioner().size(); i++) {
                //     var band = host.getBwProvisioner(i).getAvailableResource();
                //     if(band >= vm.getBw().getCapacity()) {
                //         double band_before = (double)(band)/host.getBwProvisioner(i).getCapacity();
                //         double band_after = band_before - (double)(vm.getBw().getCapacity())/host.getBwProvisioner(i).getCapacity();
                //         double dpu_value_before, dpu_value_after;
                //         if(!pointRecordDPU.containsKey(point1.getX(), point1.getY(),band_before)){
                //             dpu_value_before = pointFunctionDPU.apply(point1, band_before);
                //             pointRecordDPU.put(point1.getX(), point1.getY(), band_before, dpu_value_before);
                //         } else {
                //             dpu_value_before = pointRecordDPU.get(point1.getX(), point1.getY(), band_before);
                //         }
        
                //         if(!pointRecordDPU.containsKey(point2.getX(), point2.getY(), band_after)){
                //             dpu_value_after = pointFunctionDPU.apply(point2, band_after);
                //             pointRecordDPU.put(point2.getX(), point2.getY(), band_after, dpu_value_after);
                //         } else {
                //             dpu_value_after = pointRecordDPU.get(point2.getX(), point2.getY(), band_after);
                //         }
                //         var dpu_value_delta = dpu_value_before - dpu_value_after;
                //         if(min_delta_dpu > dpu_value_delta){
                //             vm.setNicId(i);
                //             min_delta_dpu = dpu_value_delta;
                //             band_before_final = band_before;
                //             band_after_final = band_after;
                //         }
                //     }
                // }

                // if(min_delta_dpu == Double.POSITIVE_INFINITY){
                //     System.out.printf("impossible\n");
                //     // continue;
                // }

                // if(!pointRecordNode.containsKey(point1.getX(), point1.getY(),band_before_final)){
                //     value_before = pointFunctionNode.apply(point1, band_before_final);
                //     pointRecordNode.put(point1.getX(), point1.getY(), band_before_final, value_before);
                // } else {
                //     value_before = pointRecordNode.get(point1.getX(), point1.getY(), band_before_final);
                // }

                // if(!pointRecordNode.containsKey(point2.getX(), point2.getY(), band_after_final)){
                //     value_after = pointFunctionNode.apply(point2, band_after_final);
                //     pointRecordNode.put(point2.getX(), point2.getY(), band_after_final, value_after);
                // } else {
                //     value_after = pointRecordNode.get(point2.getX(), point2.getY(), band_after_final);
                // }

                // // var delta_value = value_after - value_before;
                // var delta_value = value_before - value_after + min_delta_dpu;
                // if(delta_value < min_delta) {
                //     aim_host = Optional.of(host);
                //     min_delta = delta_value;
                // }  
                if(min_delta == Double.POSITIVE_INFINITY){
                    System.out.printf("error\n");
                    // continue;
                }
            }
        }

        return aim_host;
    }

}