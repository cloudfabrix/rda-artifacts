%% stream = no and limit = 0

## VMware vCenter: Input Credentials Source Details##
@exec:get-input
    --> @dm:save name = 'temp-variable-dataset'

## Generate Topology Nodes Ingestion ID
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> @dm:addrow vcenter_name = '$vcenter_src_name'
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> @dm:map from = 'collection_timestamp' & to = 'topology_ingestion_id' & func = 'md5'
    --> @dm:save name = 'temp-topology-nodes-ingestion-id-dict' & append = 'yes'

## VMware vCenter Inventory Nodes for Topology
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'vCenter' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'vCenter'
       --> @dm:eval iconURL = "'Infraservice'"
       --> @dm:eval node_label = "'$vcenter_src_name'"
       --> @dm:map from = 'vcenter_address,vcenter_uuid' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:save name = 'temp-processed-vcenter-inventory' & append = 'yes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## Get and create Datastore details and save it as Dictionary
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'DatastoreByHost' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:rename-columns vm_disk_datastore = 'datastore_name'
       --> @dm:dedup columns = 'vcenter_address,datacenter,datastore_host_path'
       --> @dm:save name = 'temp-datastore-details-dict' & append = 'yes'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:empty
       --> @dm:add-missing-columns columns = 'vm_disk_datastore,datastore_host_path'
       --> @dm:save name = 'temp-datastore-details-dict' & append = 'yes'
    --> @exec:end-if

## VMware vCenter Virtual Machine Nodes for Topology
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'VirtualMachineDisk' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:explode column = 'vswitch_portgroup'
       --> @dm:map attr='vswitch_portgroup' & func = 'strip'
       --> @dm:enrich dict = 'temp-datastore-details-dict' & src_key_cols = 'vm_disk_datastore' & dict_key_cols = 'vm_disk_datastore' & enrich_cols = 'datastore_host_path'
       --> @dm:map from = 'vcenter_address,datacenter,datastore_host_path' & to = 'vm_datastore_id' & func = 'join' & sep = '_'
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'VM'
       --> @dm:map from = 'vm_name' & to = 'node_label'
       --> @dm:map from = 'vcenter_address,vm_name,vm_instance_uuid' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,esxi_host,vswitch_portgroup' & to = 'vswitch_portgroup_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host' & to = 'vm_esxi_host_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-vm-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:add-missing-columns columns = 'iconURL' & value = 'Not Available'
       --> @dm:eval asset_object = "'VirtualMachine'"
       --> @dm:save name = 'temp-processed-vm-node-inventory' & append = 'yes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter ESXi Host Nodes for Topology
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'Hypervisor' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'Hypervisor'
       --> @dm:map from = 'esxi_host' & to = 'node_label'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter' & to = 'datacenter_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_cluster' & to = 'esxi_cluster_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-esxi-host-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:add-missing-columns columns = 'iconURL' & value = 'Not Available'
       --> @dm:save name = 'temp-processed-vcenter-esxi-host-nodes' & append = 'yes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter ESXi Cluster Nodes for Topology
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'Cluster' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter esxi_cluster is not empty
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'vSphere_Cluster'
       --> @dm:eval iconURL = "'Cluster'"
       --> @dm:map from = 'esxi_cluster' & to = 'node_label'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_cluster' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-esxi-cluster-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:save name = 'temp-processed-vcenter-esxi-cluster-nodes' & append = 'yes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter Datastore Nodes for Topology
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'DatastoreByHost' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'Datastore'
       --> @dm:map from = 'datastore_name' & to = 'node_label'
       --> @dm:map from = 'vcenter_address,datacenter,datastore_host_path' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host' & to = 'esxi_host_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-datastore-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:selectcolumns exclude = "^esxi_host$|^esxi_host_id$"
       --> @dm:add-missing-columns columns = 'iconURL' & value = 'Not Available'
       --> @dm:save name = 'temp-processed-vcenter-datastore-nodes' & append = 'yes'
       --> *dm:filter node_id is not empty
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter vSwitch Nodes for Topology - Standard
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'vSwitch' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *dm:safe-filter vswitch_type = 'Standard'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter esxi_host is not empty
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'vSwitch'
       --> @dm:map from = 'esxi_host,vswitch_name' & to = 'node_label' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,esxi_host,vswitch_name' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host' & to = 'esxi_host_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-processed-vcenter-esxi-host-nodes' & src_key_cols = 'esxi_host,vcenter_address' & dict_key_cols = 'esxi_host,vcenter_address' & enrich_cols = 'esxi_host_uuid'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-vswitch-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:selectcolumns exclude = 'vswitch_lldp_.*|^vswitch_network_io_control_policy$|^vswitch_version$'
       --> @dm:add-missing-columns columns = 'iconURL' & value = 'Not Available'
       --> @dm:save name = 'temp-processed-vcenter-vswitch-nodes' & append = 'yes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter vSwitch Nodes for Topology - Distributed
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'vSwitch' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *dm:safe-filter vswitch_type = 'Distributed'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter esxi_host is not empty
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'vSwitch'
       --> @dm:map from = 'vswitch_name' & to = 'node_label'
       --> @dm:map from = 'vcenter_address,datacenter,vswitch_name' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host' & to = 'esxi_host_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-dvSwitch-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:selectcolumns exclude = '^vswitch_portgroup$|^esxi_host$|^vswitch_active_pnic$|^vswitch_passive_pnic$'
       --> @dm:add-missing-columns columns = 'iconURL' & value = 'Not Available'
       --> @dm:save name = 'temp-processed-vcenter-dvSwitch-nodes' & append = 'yes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter vSwitch Consolidated output
--> @c:new-block
    --> @dm:concat names = '^temp-vcenter-vswitch-nodes$|^temp-vcenter-dvSwitch-nodes$' & return_empty = 'yes'
    --> @dm:save name = 'temp-vcenter-vswitch-consolidated-inventory'

## VMware vCenter vSwitch Portgroup Nodes for Topology - Standard
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'vSwitch' & asset_status = 'Active' & vswitch_type = 'Standard' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr='vswitch_portgroup' & func = 'strip'
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'vPortgroup'
       --> @dm:map from = 'esxi_host,vswitch_portgroup' & to = 'node_label' & func = 'join' & sep = '_'
       --> @dm:eval iconURL = "'dvPortgroup'"
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host,vswitch_name,vswitch_portgroup' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,esxi_host,vswitch_name' & to = 'vswitch_node_id' & func = 'join' & sep = '_'
       --> *dm:safe-filter vswitch_portgroup is not empty get vcenter_address,vcenter_name,datacenter,esxi_host,vswitch_name,vswitch_type,vswitch_portgroup,layer,node_id,node_label,node_type,iconURL,collection_timestamp
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-vswitch-portgroup-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:save name = 'temp-vswitch_portgroup_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter dvSwitch Portgroup Nodes for Topology - Distributed
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'vSwitch' & asset_status = 'Active' & vswitch_type = 'Distributed' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr='vswitch_portgroup' & func = 'strip'
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Virtualization'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'vPortgroup'
       --> @dm:map from = 'esxi_host,vswitch_portgroup' & to = 'node_label' & func = 'join' & sep = '_'
       --> @dm:eval iconURL = "'dvPortgroup'"
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host,vswitch_name,vswitch_portgroup' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vcenter_address,datacenter,vswitch_name' & to = 'vswitch_node_id' & func = 'join' & sep = '_'
       --> *dm:safe-filter vswitch_portgroup is not empty get vcenter_address,vcenter_name,datacenter,esxi_host,vswitch_name,vswitch_type,vswitch_portgroup,layer,node_id,node_label,node_type,iconURL,collection_timestamp
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-vswitch-portgroup-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:save name = 'temp-dvswitch_portgroup_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## VMware vCenter vSwitch Portgroups Consolidated output
--> @c:new-block
    --> @dm:concat names = '^temp-vswitch_portgroup_nodes$|^temp-dvswitch_portgroup_nodes$' & return_empty = 'yes'
    --> @dm:save name = 'temp-vcenter-vswitch-portgroups-consolidated-inventory'

## VMware vCenter Physical Switch Nodes for Topology
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'vSwitch' & asset_status = 'Active' & vcenter_name = '$vcenter_src_name' with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> *exec:if-shape num_rows > 0
       --> @dm:fixnull columns = 'vswitch_cdp_dev_name,vswitch_lldp_dev_name,vswitch_cdp_dev_ip,vswitch_cdp_dev_mgmt_ip,vswitch_cdp_dev_portid,vswitch_lldp_dev_portid'
       --> @dm:eval phy_switch_name = "''"
       --> @dm:eval phy_switch_name = "vswitch_cdp_dev_name if vswitch_cdp_dev_name != '' else vswitch_lldp_dev_name if vswitch_lldp_dev_name != '' else phy_switch_name"
       --> @dm:eval phy_switch_ip = "''"
       --> @dm:eval phy_switch_ip = "vswitch_cdp_dev_mgmt_ip if vswitch_cdp_dev_mgmt_ip != '' else vswitch_cdp_dev_ip if vswitch_cdp_dev_ip != '' else phy_switch_ip"
       --> @dm:eval phy_switch_port = "''"
       --> @dm:eval phy_switch_port = "vswitch_cdp_dev_portid if vswitch_cdp_dev_portid != '' else vswitch_lldp_dev_portid if vswitch_lldp_dev_portid != '' else phy_switch_port"
       --> *dm:filter phy_switch_name is not empty
       --> @dm:map to = 'layer' & func = 'fixed' & value = 'Network'
       --> @dm:map to = 'node_type' & func = 'fixed' & value = 'Switch'
       --> @dm:grok column = 'phy_switch_name' & pattern = "%{DATA:switch_name_temp}\(%{DATA:phy_switch_serial}\)"
       --> @dm:add-missing-columns columns = 'switch_name_temp,phy_switch_serial'
       --> @dm:fixnull columns = 'switch_name_temp'
       --> @dm:eval phy_switch_name = "switch_name_temp if switch_name_temp != '' else phy_switch_name"
       --> @dm:selectcolumns exclude = '^switch_name_temp$'
       --> @dm:map from = 'phy_switch_name' & to = 'node_label'
       --> @dm:map from = 'phy_switch_name' & to = 'node_id'
       --> @dm:eval vswitch_id = "esxi_host+'_'+vswitch_name if vswitch_type == 'Standard' else datacenter+'_'+vswitch_name if vswitch_type == 'Distributed' else vswitch_name"
       --> *dm:filter * get phy_switch_name,phy_switch_ip,vswitch_cdp_dev_platform as 'phy_switch_model',phy_switch_port,phy_switch_serial,esxi_host,vswitch_name,vswitch_type,vswitch_id,datacenter,layer,node_type,node_label,node_id,vcenter_name,vcenter_address,asset_object,collection_timestamp
       --> @dm:enrich dict = 'temp-topology-nodes-ingestion-id-dict' & src_key_cols = 'vcenter_name' & dict_key_cols = 'vcenter_name' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vcenter-phy-switch-nodes' & append = 'yes'
       --> @dm:dedup columns = 'node_id'
       --> @dm:add-missing-columns columns = 'iconURL' & value = 'Not Available'
       --> @dm:selectcolumns exclude = '^esxi_host$|^vswitch_.*$'
       --> @dm:save name = 'temp-processed-vcenter-phy-switch-nodes' & append = 'yes'
    --> @exec:end-if

## VMware vCenter - Topology Nodes
--> @c:new-block
    --> @dm:concat names = '^temp-vcenter-nodes$|^temp-vcenter-vm-nodes$|^temp-vcenter-esxi-host-nodes$|^temp-vcenter-esxi-cluster-nodes$|^temp-vcenter-datastore-nodes$|^temp-vcenter-vswitch-nodes$|^temp-vcenter-dvSwitch-nodes$|^temp-vcenter-phy-switch-nodes$' & return_empty = 'yes'
    --> @dm:save name = 'temp-vcenter-topology-nodes'

## VMware vCenter - Topology Edges
## VMware vCenter VM to ESXi Host relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'VM' get vm_esxi_host_id as 'left_id',esxi_host as 'left_label',node_id as 'right_id',vm_name as 'right_label',node_type as 'right_node_type',collection_timestamp
    --> *exec:if-shape num_rows > 0
       --> @dm:eval left_node_type = "'Hypervisor'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'runs'
       --> @dm:dedup
       --> @dm:save name = 'temp-vm-esxi-host-edges'
    --> @exec:end-if

## VMware vCenter VM to Datastore relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'VM' and vm_disk_datastore is not empty get vm_datastore_id as 'left_id',vm_disk_datastore as 'left_label',node_id as 'right_id',vm_name as 'right_label',node_type as 'right_node_type',collection_timestamp
    --> *exec:if-shape num_rows > 0
       --> @dm:eval left_node_type = "'Datastore'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'runs'
       --> @dm:dedup
       --> @dm:save name = 'temp-vm-datastore-edges'
    --> @exec:end-if

## VMware vCenter VM to vPortgroup relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'VM' and vswitch_portgroup is not empty get node_id,node_type,vcenter_address,vm_name,vm_bios_uuid,esxi_host,vm_nic_mac_address,vswitch_portgroup,datacenter,collection_timestamp
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'vcenter_address,vm_name,vm_bios_uuid,esxi_host,vm_nic_mac_address,vswitch_portgroup'
       --> @dm:enrich dict = 'temp-vcenter-vswitch-consolidated-inventory' & src_key_cols = 'esxi_host,vswitch_portgroup' & dict_key_cols = 'esxi_host,vswitch_portgroup' & enrich_cols = 'vswitch_name,vswitch_type'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host,vswitch_name,vswitch_portgroup' & to = 'vportgroup_node_id' & func = 'join' & sep = '_'
       --> *dm:filter vswitch_portgroup is not empty and vswitch_name is not empty get vportgroup_node_id as 'left_id',vswitch_portgroup as 'left_label',node_id as 'right_id',vm_name as 'right_label',node_type as 'right_node_type',collection_timestamp
       --> @dm:eval left_node_type = "'vPortgroup'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'connected-by'
       --> @dm:save name = 'temp-vm-to-vswitch-portgroup-edges'
    --> @exec:end-if

## VMware vCenter vPortgroup to vSwitch relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-vswitch-portgroups-consolidated-inventory' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'vPortgroup' and vswitch_portgroup is not empty get node_id,node_type,node_label,vcenter_address,esxi_host,vswitch_name,vswitch_portgroup,datacenter,vswitch_type,collection_timestamp
    --> *exec:if-shape num_rows > 0
       --> @dm:eval vswitch_node_id = "vcenter_address+'_'+esxi_host+'_'+vswitch_name if vswitch_type == 'Standard' else vcenter_address+'_'+datacenter+'_'+vswitch_name if vswitch_type == 'Distributed' else vswitch_name"
       --> *dm:filter vswitch_name is not empty and vswitch_portgroup is not empty get vswitch_node_id as 'left_id',vswitch_name as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type',collection_timestamp
       --> @dm:eval left_node_type = "'vSwitch'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'connected-by'
       --> @dm:save name = 'temp-vportgroup-to-vswitch-edges'
    --> @exec:end-if

## VMware vCenter vSwitch to ESXi Host relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'vSwitch'
    --> *exec:if-shape num_rows > 0
       --> @dm:add-missing-columns columns = 'vswitch_name'
       --> *dm:filter vswitch_name is not empty get node_id as 'left_id',vswitch_name as 'left_label',node_type as 'left_node_type',esxi_host_id as 'right_id',esxi_host as 'right_label',collection_timestamp
       --> @dm:eval right_node_type = "'Hypervisor'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'connects'
       --> @dm:save name = 'temp-vswitch-esxi-host-edges'
    --> @exec:end-if

## VMware vCenter vSwitch to Physical Switch relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-phy-switch-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'Switch'
    --> *exec:if-shape num_rows > 0
       --> @dm:add-missing-columns columns = 'vswitch_name,vswitch_id'
       --> *dm:filter vswitch_name is not empty get node_id as 'left_id',node_label as 'left_label',node_type as 'left_node_type',vswitch_id as 'right_id',vswitch_name as 'right_label',collection_timestamp
       --> @dm:eval right_node_type = "'vSwitch'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'connects'
       --> @dm:save name = 'temp-vswitch-physical-switch-edges'
    --> @exec:end-if

## VMware vCenter Datastore to ESXi Host relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'Datastore'
    --> *exec:if-shape num_rows > 0
       --> *dm:filter * get node_id as 'left_id',node_label as 'left_label',node_type as 'left_node_type',esxi_host_id as 'right_id',esxi_host as 'right_label',collection_timestamp
       --> @dm:eval right_node_type = "'Hypervisor'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'member-of'
       --> @dm:save name = 'temp-datastore-esxi-host-edges'
    --> @exec:end-if

## VMware vCenter ESXi Host to Cluster relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'Hypervisor' & esxi_cluster is not empty
    --> *exec:if-shape num_rows > 0
       --> *dm:filter * get node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type',esxi_cluster_id as 'left_id',esxi_cluster as 'left_label',collection_timestamp
       --> @dm:eval left_node_type = "'vSphere_Cluster'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'member-of'
       --> @dm:save name = 'temp-esxi-host-cluster-edges'
    --> @exec:end-if

## VMware vCenter ESXi Host to vCenter relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'Hypervisor' & esxi_cluster is empty
    --> *exec:if-shape num_rows > 0
       --> *dm:filter * get vcenter_uuid,vcenter_address as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type',collection_timestamp
       --> @dm:map from = 'left_label,vcenter_uuid' & to = 'left_id' & func = 'join' & sep = "_"
       --> @dm:eval left_node_type = "'vCenter'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'member-of'
       --> @dm:save name = 'temp-esxi-host-vcenter-edges'
    --> @exec:end-if

## VMware vCenter ESXi Cluster to vCenter relationship
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter node_type = 'vSphere_Cluster'
    --> *exec:if-shape num_rows > 0
       --> *dm:filter * get vcenter_uuid,vcenter_address as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type',collection_timestamp
       --> @dm:map from = 'left_label,vcenter_uuid' & to = 'left_id' & func = 'join' & sep = "_"
       --> @dm:eval left_node_type = "'vCenter'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'member-of'
       --> @dm:save name = 'temp-esxi-cluster-vcenter-edges'
    --> @exec:end-if

## Generate Topology Edges Ingestion ID
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> @dm:addrow inventory_source = '$vcenter_src_name'
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> @dm:map from = 'collection_timestamp' & to = 'topology_ingestion_id' & func = 'md5'
    --> @dm:save name = 'temp-topology-edges-ingestion-id-dict' & append = 'yes'

## VMware vCenter Inventory - Edges
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:concat names = '^temp-vm-esxi-host-edges$|^temp-vm-datastore-edges$|^temp-vm-to-vswitch-portgroup-edges$|^temp-vportgroup-to-vswitch-edges$|^temp-vswitch-esxi-host-edges$|^temp-vswitch-physical-switch-edges$|^temp-datastore-esxi-host-edges$|^temp-esxi-host-cluster-edges$|^temp-esxi-cluster-vcenter-edges$|^temp-esxi-host-vcenter-edges$' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'left_id' & func = 'strip'
       --> @dm:map attr = 'right_id' & func = 'strip'
       --> @dm:map from = 'left_id' & to = 'left'
       --> @dm:map from = 'right_id' & to = 'right'
       --> @dm:eval inventory_source = "'$vcenter_src_name'"
       --> *dm:filter left_id != right_id
       --> *dm:filter left_label is not empty
       --> *dm:filter right_label is not empty
       --> @dm:dedup columns = 'left_id,right_id'
       --> @dm:enrich dict = 'temp-topology-edges-ingestion-id-dict' & src_key_cols = 'inventory_source' & dict_key_cols = 'inventory_source' & enrich_cols = 'topology_ingestion_id'
       --> @dm:save name = 'temp-vmware-vcenter-topology-edges'
       --> *dm:safe-filter * get left_label,left_id,left_node_type,relation_type,right_id,right_label,right_node_type,inventory_source,topology_ingestion_id,collection_timestamp
       --> @rn:write-stream name = 'cfx_rdaf_topology_edges'
       --> @graph:insert-edges dbname = 'cfx_rdaf_topology' & nodes_collection = 'cfx_rdaf_topology_nodes' & edges_collection = 'cfx_rdaf_topology_edges' & left_id = 'left_id' & right_id = 'right_id'
    --> @exec:end-if

## Delete Stale vCenter Topology Nodes
--> @c:new-block
    --> @dm:empty
    --> @dm:sleep seconds = 60

## Delete Stale VMware vCenter Topology Nodes from Pstream
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id'
       --> @exec:for-loop num_rows = 1
          --> #dm:pstream-delete-data-by-query topology_ingestion_id != '{{row.topology_ingestion_id}}' & vcenter_name = '{{row.vcenter_name}}' with-input name = 'cfx_rdaf_topology_nodes' & timeout = 300
          --> @dm:save name = 'temp-vcenter_deleted_nodes'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale vCenter Topology Edges from Pstream
--> @c:new-block
    --> @dm:recall name = 'temp-vmware-vcenter-topology-edges' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id'
       --> @exec:for-loop num_rows = 1
          --> #dm:pstream-delete-data-by-query topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}' with-input name = 'cfx_rdaf_topology_edges' & timeout = 300
          --> @dm:save name = 'temp-vcenter_deleted_edges'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale VMware vCenter Topology Nodes from GraphDB
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-topology-nodes' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id'
       --> @exec:for-loop num_rows = 1
          --> @graph:delete-by-query dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & delete_query = "topology_ingestion_id != '{{row.topology_ingestion_id}}' & vcenter_name = '{{row.vcenter_name}}'"
          --> @dm:save name = 'temp-vcenter_deleted_nodes_from_graphdb'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale vCenter Topology Edges from GraphDB
--> @c:new-block
    --> @dm:recall name = 'temp-vmware-vcenter-topology-edges' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id'
       --> @exec:for-loop num_rows = 1
          --> @graph:delete-by-query dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_edges' & delete_query = "topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}'"
          --> @dm:save name = 'temp-vcenter_deleted_edges_from_graphdb'
       --> @exec:end-loop
    --> @exec:end-if

