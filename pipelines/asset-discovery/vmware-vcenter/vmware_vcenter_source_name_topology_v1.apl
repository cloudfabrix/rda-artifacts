%% stream = no and limit = 0

%% import_source = "vcenter_prod"

## VMware vCenter VM Topology
@c:new-block
    --> @dm:empty
    --> @dm:addrow vcenter_src_name='vcenter_prod'
    --> @exec:run-pipeline name = 'vmware_vcenter_topology_pipeline'
