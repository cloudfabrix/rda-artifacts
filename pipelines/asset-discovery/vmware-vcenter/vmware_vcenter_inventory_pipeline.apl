%% stream = no and limit = 0

## VMware vCenter: Input Credentials Source Details##
@exec:get-input
    --> @dm:save name = 'temp-variable-dataset'

## VMware vCenter Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:vcenter-summary skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'vcenter-summary'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:eval vcenter_name = "'$vcenter_src_name'"
       --> @dm:save name = 'temp-vmware_vcenter_summary_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'vcenter-summary'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter VMs Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:vms retry_count=3 and retry_wait_time=30 & skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'vms'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_vms_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'vms'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter Hosts Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:hosts retry_count=3 and retry_wait_time=30 & skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'hosts'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_esxi_hosts_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'hosts'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter Clusters Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:clusters skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'clusters'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_esxi_clusters_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'clusters'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter Datastores Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:datastores retry_count=3 & retry_wait_time=30 & skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'datastores'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_datastores_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'datastores'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter vSwiches Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:vswitches retry_count=3 & retry_wait_time=30 & skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'vswitches'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_vswitches_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'vswitches'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter Storage Adapters Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:host-storage-adapters retry_count=3 & retry_wait_time=30 & skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'host-storage-adapters'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_storage_adapters_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'host-storage-adapters'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## VMware vCenter VMKernel Network Inventory
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> *$vcenter_src_name:host-networks retry_count=3 & retry_wait_time=30 & skip_on_error = 'yes'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map to = 'collection_timestamp' & func = 'time_now'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'host-networks'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-vmware_host_networks_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$vcenter_src_name'"
       --> @dm:eval bot_source_type = "'vmware_vcenter'"
       --> @dm:eval bot_name = "'host-networks'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Get Host OS System Inventory for Enrichment
--> @c:new-block
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'System' with-input name = 'host-os-inventory' & limit = 0 & skip_error = 'yes'
    --> @dm:save name = 'temp-host_os_system_inventory'

## VMware vCenter Inventory Data Processing
## 
## VMware vCenter Summary Data Processing
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_vcenter_summary_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'vCenter'
       --> *dm:safe-filter * get vcenter_name,
               vcenter_address,
               fullName as 'vcenter_full_version',
               build as 'vcenter_version_build',
               version as 'vcenter_short_version',
               osType as 'vcenter_os_type',
               instanceUuid as 'vcenter_uuid',
               cpucapacity as 'cpu_capacity_ghz',
               cpuused as 'cpu_used_ghz',
               cpufree as 'cpu_free_ghz',
               memorycapacity as 'memory_capacity_gb',
               memoryused as 'memory_used_gb',
               memoryfree as 'memory_free_gb',
               storagecapacity as 'storage_capacity_gb',
               storageused as 'storage_used_gb',
               storagefree as 'storage_free_gb',
               num_of_datastores,
               num_of_hosts,
               num_of_networks,
               num_of_vms,
               asset_object,
               asset_status,
               collection_timestamp
       --> @dm:map from = 'vcenter_address,vcenter_uuid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-processed-vcenter-inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## ###################################
## VMware vCenter VMs Data Processing
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_vms_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> *dm:safe-filter * get name as 'vm_name',
               guest as 'vm_guest_os',
               powerState as 'vm_power_state',
               ipAddress as 'vm_ip_address',
               vm_nic_macAddress as 'vm_nic_mac_address',
               network_adapters as 'vm_nic_additional_mac_temp',
               id as 'vm_bios_uuid',
               vm_id as 'vm_vcenter_id',
               instance_uuid as 'vm_instance_uuid',
               vm_nic_ipaddresses as 'vm_additional_ips',
               path as 'vm_config_path',
               numCpu as 'vm_cpus',
               numsockets as 'vm_cpu_sockets',
               numcorespersocket as 'vm_cores_per_socket',
               cpushares as 'vm_cpu_shares',
               cpuReservation_MHz as 'vm_cpu_reservation_mhz',
               cpureservationlimit_MHz as 'vm_cpu_limit_mhz',
               memorySizeMB as 'vm_memory_mb',
               memoryshares as 'vm_memory_shares',
               memoryReservation_MB as 'vm_memory_reservation_mb',
               memoryReservationlimit_MB as 'vm_memory_limit_mb',
               network as 'vswitch_portgroup',
               storage_name as 'datastore_name',
               toolsRunningStatus as 'vm_tools_status',
               template as 'vm_template',
               version as 'vm_hw_version',
               vm_nic_connected as 'vm_nic_status',
               datacenter as 'datacenter',
               host as 'esxi_host',
               cluster as 'esxi_cluster',
               resourcePool as 'resourcepool',
               disk_backing_file as 'vm_disk_path',
               disk_bus_num as 'vm_disk_bus_num',
               disk_capacity_Bytes as 'vm_disk_capacity_bytes',
               disk_controller as 'vm_disk_controller',
               disk_controller_type as 'vm_disk_controller_type',
               disk_datastore as 'vm_disk_datastore',
               disk_diskMode as 'vm_disk_mode',
               disk_eagerlyScrub as 'vm_disk_scrub',
               disk_label as 'vm_disk_label',
               disk_split as 'vm_disk_split',
               disk_thinProvisioned as 'vm_disk_thinprovisioned',
               disk_unit_num as 'vm_disk_number',
               disk_writeThrough as 'vm_disk_writethrough',
               disk_compatibilityMode as 'vm_disk_compatibility_mode',
               disk_lunUuid as 'vm_disk_lun_uuid',
               numEthernetCards as 'vm_nics',
               numVirtualDisks as 'vm_disks',
               asset_object,
               vcenter_address,
               collection_timestamp,
               asset_status
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:map attr = 'vm_nic_status' & func = 'lower'
       --> @dm:map attr = 'vm_disk_split' & func = 'lower'
       --> @dm:map attr = 'vm_disk_thinprovisioned' & func = 'lower'
       --> @dm:map attr = 'vm_disk_writethrough' & func = 'lower'
       --> @dm:map attr = 'vm_disk_scrub' & func = 'lower'
       --> @dm:map attr = 'vm_nic_mac_address' & func = 'lower'
       --> @dm:map attr = 'vm_nic_additional_mac_temp' & func = 'lower'
       --> @dm:to-type columns = 'vm_cpus,vm_cpu_sockets,vm_cores_per_socket,vm_cpu_shares,vm_memory_mb,vm_memory_shares,vm_disk_capacity_bytes' & type = 'int'
       --> @dm:fixnull columns = 'vm_disk_capacity_bytes,vm_disk_compatibility_mode,vm_disk_mode,vm_disk_lun_uuid'
       --> @dm:eval vm_disk_rdm = "'Yes' if vm_disk_lun_uuid != '' else 'No'"
       --> @dm:map to = 'vm_disk_capacity_gb' & func = 'fixed' & value = 0
       --> @dm:eval vm_disk_capacity_gb = "round(vm_disk_capacity_bytes / 1024 / 1024 / 1024, 0) if vm_disk_capacity_bytes else vm_disk_capacity_gb"
       --> @dm:eval vm_guest_os_type = "'Windows' if 'Microsoft' in vm_guest_os else 'VMware ESXi' if 'VMware' in vm_guest_os else 'Linux'"
       --> @dm:save name = 'temp-vcenter_vm_processed_inventory'
    --> @exec:end-if

## VMware vCenter Virtual Machines - IP and MAC Consolidation Dictionary
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter_vm_processed_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:explode column = 'vm_additional_ips'
       --> *dm:safe-filter vm_additional_ips does not contain '::'
       --> @dm:explode column = 'vm_nic_additional_mac_temp'
       --> *dm:filter vm_nic_additional_mac_temp contains 'mac_address'
       --> @dm:grok column = 'vm_nic_additional_mac_temp' & pattern = "%{MAC:vm_nic_additional_mac_address}"
       --> @dm:implode key_columns = 'vcenter_address,vm_bios_uuid,vm_vcenter_id' & merge_columns = 'vm_additional_ips,vm_nic_mac_address,vm_nic_additional_mac_address'
       --> @dm:save name = 'temp-vcenter_vm_nic_consolidated_nic_mac_dict'
    --> @exec:end-if

## VMware vCenter Virtual Machines Unique Records
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter_vm_processed_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'VirtualMachine'
       --> @dm:selectcolumns exclude = '^vm_additional_ips$|^vm_nic_mac_address$|^vm_nic_additional_mac_temp$'
       --> @dm:enrich dict = 'temp-vcenter_vm_nic_consolidated_nic_mac_dict' & src_key_cols = 'vcenter_address,vm_bios_uuid,vm_vcenter_id' & dict_key_cols = 'vcenter_address,vm_bios_uuid,vm_vcenter_id' & enrich_cols = 'vm_additional_ips,vm_nic_mac_address,vm_nic_additional_mac_address'
       --> @dm:map from = 'vcenter_address,vm_name,vm_instance_uuid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:selectcolumns exclude = 'vm_disk_.*'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:enrich dict = 'temp-host_os_system_inventory' & src_key_cols = 'vm_bios_uuid' & dict_key_cols = 'host_bios_uuid' & enrich_cols = 'host_os_ip,hostname,host_os_version' & return_empty_dict = 'yes' & return_empty_cols = 'yes'
       --> @dm:add-missing-columns columns = 'host_bios_uuid'
       --> @dm:fixnull columns = 'host_os_ip,hostname,host_os_version,host_bios_uuid' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:save name = 'temp-vcenter_unique_vm_inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## VMware vCenter Virtual Machine Disk Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter_vm_processed_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'VirtualMachineDisk'
       --> @dm:map from = 'vcenter_address,vm_name,vm_instance_uuid,vm_disk_path' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-vcenter_unique_vm_disk_inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## Process VM Network Configuration to capture MAC address and IP Address for Topology
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter_unique_vm_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter vm_nic_additional_mac_address is not empty get vm_name as 'asset_name',vcenter_address as 'inventory_source',vm_nic_additional_mac_address as 'endpoint_address',datacenter,vm_instance_uuid,collection_timestamp
       --> @dm:eval asset_type = "'VirtualMachine'" & endpoint_address_type = "'MAC'"
       --> @dm:explode column = 'endpoint_address'
       --> @dm:map attr = 'endpoint_address' & func = 'strip'
       --> @dm:map from = 'inventory_source,asset_name,vm_instance_uuid' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'inventory_source,asset_name,endpoint_address_type,endpoint_address' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:selectcolumns exclude = "^datacenter$|^vm_instance_uuid$"
       --> @dm:fixnull-regex columns = '.*' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-vm_network_mac_ip_processed'
       --> @rn:write-stream name = 'network-endpoints-identity-stream'
    --> @exec:end-if

## ##########################################
## VMware vCenter ESXi Hosts Data Processing
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_esxi_hosts_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'Hypervisor'
       --> *dm:safe-filter * get vcenter_address,
               datacenter,
               cluster as 'esxi_cluster',
               host as 'esxi_host',
               dnsconfighostname as 'esxi_hostname',
               dnsconfigdomainname as 'esxi_domainame',
               dnsserveripaddress as 'esxi_host_dns_ips',
               powerState as 'esxi_host_powerstate',
               hardware_bios_uuid as 'esxi_host_uuid',
               fullName as 'esxi_host_version',
               hardware_vendor as 'esxi_host_hw_vendor',
               esx_host_hardware_model as 'esxi_host_hw_model',
               esxi_host_hw_ServiceTag as 'esxi_host_hw_serial',
               cluster_drs_enabled as 'esxi_host_drs_status',
               cluster_ha_enabled as 'esxi_host_ha_status',
               connectionState as 'esxi_host_connection_status',
               iproutedefaultGateway as 'esxi_host_gw_ip',
               accessible_datastores as 'esxi_host_datastores',
               inaccessible_datastores as 'esxi_host_inaccessible_datastores',
               hyperthreadInfoactive as 'esxi_host_hyperthreading',
               iscsiSupported as 'esxi_host_iscsi_support',
               inMaintenanceMode as 'esxi_host_maintenance_status',
               cpuModel as 'esxi_host_cpu_model',
               numCpuPkgs as 'esxi_host_cpu_sockets',
               esx_host_num_cpu_cores as 'esxi_host_cpu_cores',
               esx_host_num_cpu_threads as 'esxi_host_cpu_threads',
               memorySize as 'esxi_host_memory_bytes',
               numHBAs as 'esxi_host_hbas',
               numNics as 'esxi_host_nics',
               no_of_vms as 'esxi_host_vms',
               asset_object,
               collection_timestamp,
               asset_status
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:map attr = 'esxi_host_drs_status' & func = 'lower'
       --> @dm:map attr = 'esxi_host_ha_status' & func = 'lower'
       --> @dm:map attr = 'esxi_host_hyperthreading' & func = 'lower'
       --> @dm:map attr = 'esxi_host_iscsi_support' & func = 'lower'
       --> @dm:to-type columns = 'esxi_host_cpu_sockets,esxi_host_cpu_cores,esxi_host_cpu_threads,esxi_host_hbas,esxi_host_nics,esxi_host_vms,esxi_host_memory_bytes' & type = 'int'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_host' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-processed-esxi-host-inventory'
       --> @dm:sleep seconds = 60
       --> @dm:recall name = 'temp-processed-vcenter-inventory'
       --> @exec:for-loop num_rows = 1
          --> #dm:query-persistent-stream asset_object = 'VirtualMachine' & vcenter_address = '{{row.vcenter_address}}' with-input name = 'vmware-vcenter-inventory' & limit = 0
          --> *exec:if-shape num_rows > 0
             --> @dm:dedup columns = 'vcenter_address,vm_name,vm_instance_uuid'
             --> *dm:safe-filter vm_memory_mb is not empty
             --> @dm:fixnull columns = 'vm_memory_mb'
             --> @dm:to-type columns = 'vm_cpus,vm_memory_mb' & type = 'int'
             --> @dm:eval-multi-proc vm_memory_gb = "round(int(vm_memory_mb) / 1024, 0)"
             --> @dm:groupby columns = 'esxi_host,esxi_cluster,vcenter_address' & agg = 'sum'
             --> @dm:save name = 'temp-vm_cpus_per_host_dict'
             --> @dm:recall name = 'temp-processed-esxi-host-inventory'
             --> @dm:dedup columns = 'esxi_host_uuid,esxi_host'
             --> @dm:enrich dict = 'temp-vm_cpus_per_host_dict' & src_key_cols = 'esxi_host' & dict_key_cols = 'esxi_host' & enrich_cols = 'vm_cpus,vm_memory_gb' & enrich_cols_as = 'vm_total_vcpus,vm_total_memory_gb'
             --> *dm:safe-filter esxi_host is not empty
             --> @dm:eval vcpu_per_cpu_core = "0 if vm_total_vcpus == '' else round(vm_total_vcpus / esxi_host_cpu_cores, 1)"
             --> *dm:safe-filter * get esxi_host,vcpu_per_cpu_core,vm_total_vcpus,vm_total_memory_gb
             --> @dm:save name = 'temp-esxi_host_vcpu_usage_dict'
             --> @dm:recall name = 'temp-processed-esxi-host-inventory'
             --> @dm:enrich dict = 'temp-esxi_host_vcpu_usage_dict' & src_key_cols = 'esxi_host' & dict_key_cols = 'esxi_host' & enrich_cols = 'vcpu_per_cpu_core,vm_total_vcpus,vm_total_memory_gb' & enrich_cols_as = 'esxi_host_vcpus_per_core,esxi_host_vcpus,esxi_host_provisioned_memory_gb'
             --> @dm:to-type columns = 'esxi_host_provisioned_memory_gb,esxi_host_vcpus,esxi_host_vms' & type = 'int'
             --> @dm:eval esxi_host_vcpus = "0 if esxi_host_vms == 0 else esxi_host_vcpus"
             --> @dm:eval esxi_host_provisioned_memory_gb = "0 if esxi_host_vms == 0 else esxi_host_provisioned_memory_gb"
             --> @dm:eval esxi_host_cpu_threads = "esxi_host_cpu_threads / esxi_host_cpu_cores"
             --> @dm:eval esxi_host_cpus = "esxi_host_cpu_cores * esxi_host_cpu_threads"
             --> @dm:eval esxi_host_memory_gb = "round(esxi_host_memory_bytes / 1024 / 1024 / 1024, 0)"
             --> @dm:fixnull columns = 'esxi_host_memory_gb,esxi_host_provisioned_memory_gb' & value = 0
             --> @dm:map to = 'esxi_host_available_memory_gb' & func = 'fixed' & value = 0
             --> @dm:to-type columns = 'esxi_host_available_memory_gb' & type= 'int'
             --> @dm:eval esxi_host_available_memory_gb = "int(esxi_host_memory_gb) - int(esxi_host_provisioned_memory_gb) if esxi_host_provisioned_memory_gb != 0 else esxi_host_available_memory_gb"
             --> @dm:eval esxi_host_overprovision_memory_status = "'Yes' if esxi_host_provisioned_memory_gb and int(esxi_host_available_memory_gb) < 0 else 'No'"
             --> @dm:eval esxi_host_overprovision_memory_perc = "round(((int(esxi_host_provisioned_memory_gb) * 100) / esxi_host_memory_gb) - 100, 0) if esxi_host_overprovision_memory_status == 'Yes' else '0'"
             --> @dm:eval-multi-proc esxi_host_available_memory_gb = "esxi_host_available_memory_gb if esxi_host_available_memory_gb > 0 else 0"
             --> @dm:fixnull columns = 'esxi_host_hw_vendor'
             --> @dm:eval esxi_host_hw_vendor = "'Unknown' if esxi_host_hw_vendor == '' else esxi_host_hw_vendor"
             --> @dm:save name = 'temp-processed-esxi-host-with-capacity-inventory'
             --> @rn:write-stream name = 'vmware-vcenter-inventory'
          --> @exec:end-if
          --> *exec:if-shape num_rows = 0
             --> @dm:recall name = 'temp-processed-esxi-host-inventory'
             --> @rn:write-stream name = 'vmware-vcenter-inventory'
          --> @exec:end-if
       --> @exec:end-loop
    --> @exec:end-if

## #######################################################
## VMware vCenter vSphere Clusters Inventory##
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_esxi_clusters_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'Cluster'
       --> *dm:safe-filter * get vcenter as 'vcenter_address',
               admissionControlEnabled as 'ha_admission_control',
               cluster as 'esxi_cluster',
               clusterSettings as 'cluster_settings',
               cpucapacity as 'cpu_capacity',
               cpucores as 'cpu_cores',
               cpufree as 'cpu_free',
               cputhreads as 'cpu_threads',
               cpuused as 'cpu_used',
               datacenter,
               defaultdpmbehavior as 'dpm_automation',
               drsautomationlevel as 'drs_automation',
               drsdefaultVmBehavior as 'drs_vm_automation',
               drsenabled as 'drs_status',
               drspowermgtmenabled as 'dpm_status',
               effectivecpu as 'cluster_effective_cpu',
               effectivehosts as 'cluster_effective_hosts',
               effectivememory as 'cluster_effective_memory',
               enableAPDTimeoutForHosts as 'apd_timeout_for_hosts',
               evcenabled as 'evc_status',
               failoverLevel as 'cluster_failover_level',
               failureInterval as 'cluster_failover_interval',
               hBDatastoreCandidatePolicy as 'heartbeat_datastore_policy',
               haenabled as 'ha_status',
               heartbeatDatastore as 'heartbeat_datastore',
               hostmonitoring as 'host_monitoring_status',
               hosts as 'host_count',
               isolationResponse as 'ha_isolation_response',
               maxFailureWindow as 'ha_max_failure_window',
               maxFailures as 'ha_max_failures',
               memorycapacity as 'memory_capacity',
               memoryfree as 'memory_free',
               memoryused as 'memory_used',
               ntpserverconfigured as 'ntp_server_status',
               ntpserversmessage as 'ntp_server_config',
               numvmotions as 'vmotions_count',
               overallstatus as 'cluster_health',
               resourceReductionToToleratePercent as 'resource_reduction_tolerate_perc',
               resourcepoolstatus as 'resource_pool_health',
               restartPriority as 'ha_restart_priority',
               restartPriorityTimeout as 'ha_restart_priority_timeout',
               spbmenabled as 'storage_policy_management',
               storagecapacity as 'storage_capacity',
               storageused as 'storage_used',
               storagefree as 'storage_free',
               vmToolsMonitoringSettingsvmMonitoring as 'vm_tools_monitoring',
               vmmonitoring as 'vm_monitoring',
               vmotioncompatiblecpus as 'vmotion_compatible_cpus',
               vmswapplacement as 'vm_swap_location',
               vsanenabled as 'vsan_status',
               collection_timestamp,
               asset_status,
               asset_object
       --> @dm:map attr = 'ha_admission_control' & func = 'lower'
       --> @dm:map attr = 'cluster_settings' & func = 'lower'
       --> @dm:map attr = 'drs_status' & func = 'lower'
       --> @dm:map attr = 'dpm_status' & func = 'lower'
       --> @dm:map attr = 'apd_timeout_for_hosts' & func = 'lower'
       --> @dm:map attr = 'evc_status' & func = 'lower'
       --> @dm:map attr = 'ha_status' & func = 'lower'
       --> @dm:map attr = 'ntp_server_status' & func = 'lower'
       --> @dm:map attr = 'storage_policy_management' & func = 'lower'
       --> @dm:map attr = 'vmotion_compatible_cpus' & func = 'lower'
       --> @dm:map attr = 'vsan_status' & func = 'lower'
       --> @dm:eval cpu_capacity = "round(cpu_capacity, 0)" & cpu_free = "round(cpu_free, 0)" & cpu_used = "round(cpu_used, 0)" & memory_capacity = "round(memory_capacity, 0)" & memory_free = "round(memory_free, 0)" & memory_used = "round(memory_used, 0)" & storage_capacity = "round(storage_capacity, 0)" & storage_used = "round(storage_used, 0)" & storage_free = "round(storage_free, 0)"
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:map from = 'vcenter_address,datacenter,esxi_cluster' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-processed-vcenter-cluster-inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## ##############
## VMware vCenter Datastores Inventory
## 
## Get VMs with Disk Capacity Details for Enrichment
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'VirtualMachineDisk' & vcenter_name = '$vcenter_src_name'  with-input name = 'vmware-vcenter-inventory' & limit = 0
    --> @dm:save name = 'temp-all_vms_with_disks_configuration_dict' & append = 'yes'

## Powered On VMs Disk Capacity per Datastore
--> @c:new-block
    --> @dm:recall name = 'temp-all_vms_with_disks_configuration_dict' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter vm_power_state = 'poweredOn'
       --> @dm:groupby columns = 'vcenter_name,vcenter_address,vm_disk_datastore,datacenter' & agg = 'sum'
       --> *dm:safe-filter * get vcenter_name,vcenter_address,datacenter,vm_disk_datastore,vm_disk_capacity_gb as 'vm_on_capacity_gb'
       --> @dm:save name = 'temp-datastore_vm_on_capacity'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:empty
       --> @dm:add-missing-columns columns = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore,vm_on_capacity_gb'
       --> @dm:save name = 'temp-datastore_vm_on_capacity'
    --> @exec:end-if

## Powered Off VMs Disk Capacity per Datastore
--> @c:new-block
    --> @dm:recall name = 'temp-all_vms_with_disks_configuration_dict' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter vm_power_state = 'poweredOff'
       --> @dm:groupby columns = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore' & agg = 'sum'
       --> *dm:safe-filter * get vcenter_name,vcenter_address,datacenter,vm_disk_datastore,vm_disk_capacity_gb as 'vm_off_capacity_gb'
       --> @dm:save name = 'temp-datastore_vm_off_capacity'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:empty
       --> @dm:add-missing-columns columns = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore,vm_off_capacity_gb'
       --> @dm:save name = 'temp-datastore_vm_on_capacity'
    --> @exec:end-if

## All VMs Disk Capacity per Datastore
--> @c:new-block
    --> @dm:recall name = 'temp-all_vms_with_disks_configuration_dict' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:groupby columns = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore' & agg = 'sum'
       --> *dm:safe-filter * get vcenter_name,vcenter_address,datacenter,vm_disk_datastore,vm_disk_capacity_gb as 'all_vms_capacity_gb'
       --> @dm:save name = 'temp-datastore_all_vms_capacity'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:empty
       --> @dm:add-missing-columns columns = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore,all_vms_capacity_gb'
       --> @dm:save name = 'temp-datastore_vm_on_capacity'
    --> @exec:end-if

## VMware vCenter Datastores Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_datastores_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'DatastoreByHost'
       --> *dm:safe-filter * get vcenter_address,
               datacenter,
               name as 'datastore_name',
               capacity_GB as 'datastore_total_gb',
               freeSpace_GB as 'datastore_free_gb',
               provisioned_GB as 'datastore_provisioned_gb',
               overprovisioned_pct as 'datastore_overprovisioned_pct',
               extents as 'datastore_extents',
               ds_cluster_name as 'datastore_cluster_name',
               ds_cluster_datastores as 'datastore_cluster_members',
               ds_cluster_overallStatus as 'datastore_cluster_status',
               host_accessMode as 'datastore_host_access_mode',
               host_accessible as 'datastore_host_access_status',
               host_mounted as 'datastore_host_mounted_status',
               host_name as 'esxi_host',
               host_path as 'datastore_host_path',
               local as 'datastore_local',
               multipleHostAccess as 'datastore_multihost_access_status',
               num_of_hosts as 'datastore_hosts',
               remoteHost as 'datastore_remote_host',
               remotePath as 'datastore_remote_path',
               ssd as 'datastore_ssd_status',
               type as 'datastore_type',
               uuid as 'datastore_uuid',
               vmfs_version as 'datastore_vmfs_version',
               asset_object,
               collection_timestamp,
               asset_status
       --> @dm:map attr = 'datastore_host_access_status' & func = 'lower'
       --> @dm:map attr = 'datastore_multihost_access_status' & func = 'lower'
       --> @dm:map attr = 'datastore_host_mounted_status' & func = 'lower'
       --> @dm:map attr = 'datastore_ssd_status' & func = 'lower'
       --> @dm:map attr = 'datastore_local' & func = 'lower'
       --> @dm:to-type columns = 'datastore_total_gb,datastore_free_gb,datastore_provisioned_gb' & type = 'int'
       --> @dm:eval datastore_used_gb = "(datastore_total_gb - datastore_free_gb) if datastore_total_gb > 0 else 0"
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:map from = 'vcenter_address,esxi_host,datastore_name,datastore_uuid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-processed-datastore-inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## Datastores with VM Disk Capacity Enrichment
--> @c:new-block
    --> @dm:recall name = 'temp-all_vms_with_disks_configuration_dict' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:empty
       --> @dm:recall name = 'temp-processed-datastore-inventory'
       --> @dm:dedup columns = 'vcenter_name,vcenter_address,datacenter,datastore_name,datastore_uuid'
       --> @dm:enrich dict = 'temp-datastore_vm_on_capacity' & src_key_cols = 'vcenter_name,vcenter_address,datacenter,datastore_name' & dict_key_cols = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore' & enrich_cols = 'vm_on_capacity_gb' & enrich_cols_as = 'powered_on_vms_capacity_gb'
       --> @dm:selectcolumns exclude = '^vm_disk_datastore$'
       --> @dm:enrich dict = 'temp-datastore_vm_off_capacity' & src_key_cols = 'vcenter_name,vcenter_address,datacenter,datastore_name' & dict_key_cols = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore' & enrich_cols = 'vm_off_capacity_gb' & enrich_cols_as = 'powered_off_vms_capacity_gb'
       --> @dm:selectcolumns exclude = '^vm_disk_datastore$'
       --> @dm:enrich dict = 'temp-datastore_all_vms_capacity' & src_key_cols = 'vcenter_name,vcenter_address,datacenter,datastore_name' & dict_key_cols = 'vcenter_name,vcenter_address,datacenter,vm_disk_datastore' & enrich_cols = 'all_vms_capacity_gb'
       --> @dm:selectcolumns exclude = '^vm_disk_datastore$'
       --> @dm:add-missing-columns columns = 'powered_on_vms_capacity_gb,powered_off_vms_capacity_gb,all_vms_capacity_gb'
       --> @dm:fixnull columns = 'powered_on_vms_capacity_gb,powered_off_vms_capacity_gb,all_vms_capacity_gb'
       --> @dm:eval powered_on_vms_capacity_gb = "int(powered_on_vms_capacity_gb) if powered_on_vms_capacity_gb != '' else powered_on_vms_capacity_gb" & powered_off_vms_capacity_gb = "int(powered_off_vms_capacity_gb) if powered_off_vms_capacity_gb != '' else powered_off_vms_capacity_gb" & all_vms_capacity_gb = "int(all_vms_capacity_gb) if all_vms_capacity_gb != '' else all_vms_capacity_gb"
       --> @dm:eval datastore_unaccounted_capacity_status = "'Yes' if all_vms_capacity_gb != '' and datastore_provisioned_gb > all_vms_capacity_gb  else 'No'"
       --> @dm:save name = 'temp-processed-datastore-with-vm-disk-capacity-enrichment'
       --> @dm:recall name = 'temp-processed-datastore-inventory'
       --> @dm:enrich dict = 'temp-processed-datastore-with-vm-disk-capacity-enrichment' & src_key_cols = 'vcenter_name,vcenter_address,datacenter,datastore_name' & dict_key_cols = 'vcenter_name,vcenter_address,datacenter,datastore_name' & enrich_cols = 'all_vms_capacity_gb,powered_on_vms_capacity_gb,powered_off_vms_capacity_gb,datastore_unaccounted_capacity_status'
       --> @dm:fixnull columns = 'powered_on_vms_capacity_gb,powered_off_vms_capacity_gb,all_vms_capacity_gb'
       --> @dm:eval powered_on_vms_capacity_gb = "int(powered_on_vms_capacity_gb) if powered_on_vms_capacity_gb != '' else 0" & powered_off_vms_capacity_gb = "int(powered_off_vms_capacity_gb) if powered_off_vms_capacity_gb != '' else 0" & all_vms_capacity_gb = "int(all_vms_capacity_gb) if all_vms_capacity_gb != '' else 0"
       --> @dm:dedup columns = 'vcenter_name,vcenter_address,datacenter,datastore_name,datastore_uuid'
       --> @dm:selectcolumns exclude = "^unique_id$|^asset_status$|^asset_object$|^esxi_host$"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'Datastore'
       --> @dm:map from = 'vcenter_address,datacenter,datastore_host_path' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
       --> @dm:sleep seconds = '30'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:empty
       --> @dm:recall name = 'temp-processed-datastore-inventory'
       --> @dm:dedup columns = 'vcenter_name,vcenter_address,datacenter,datastore_name,datastore_uuid'
       --> @dm:selectcolumns exclude = "^unique_id$|^asset_status$|^asset_object$|^esxi_host$"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'Datastore'
       --> @dm:map from = 'vcenter_address,datacenter,datastore_host_path' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
       --> @dm:sleep seconds = '30'
    --> @exec:end-if

## SAN Fabric Switch Inventory Dict for Enrichment
--> @c:new-block
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'NameServer' and switch_chassis_wwn is not empty with-input name = 'san-switch-fabric-inventory' & limit = 0 & skip_error = 'yes'
    --> @dm:save name = 'temp-san_switch_fabric_nameserver_inventory'

## #########################
## 
## VMware vCenter Storage Adapters Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_storage_adapters_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'StorageAdapter'
       --> *dm:safe-filter * get vcenter_address,
               datacenter,
               cluster as 'esxi_cluster',
               host as 'esxi_host',
               device as 'esxi_hba_name',
               driver as 'esxi_hba_driver',
               model as 'esxi_hba_model',
               iScsiAlias as 'esxi_hba_iscsi_alias',
               iScsiName as 'esxi_hba_iscsi_name',
               path_transport_address as 'esxi_hba_iscsi_target_address',
               path_transport_iScsiName as 'esxi_hba_iscsi_target_iqn',
               nodeWorldWideName as 'esxi_hba_wwnn',
               portWorldWideName as 'esxi_hba_wwpn',
               path_devicePath as 'esxi_disk_device_path',
               path_displayName as 'esxi_disk_device_name',
               path_localDisk as 'esxi_disk_device_local',
               path_name as 'esxi_disk_device_id',
               path_operationalState as 'esxi_disk_device_path_status',
               path_pathState as 'esxi_disk_device_path_mode',
               path_transport_nodeWorldWideName as 'esxi_disk_device_target_wwnn',
               path_transport_portWorldWideName as 'esxi_disk_device_target_wwpn',
               path_uuid as 'esxi_disk_device_uuid',
               path_vendor as 'esxi_disk_device_vendor',
               speed as 'esxi_disk_device_path_speed',
               status as 'esxi_device_status',
               asset_object,
               collection_timestamp,
               asset_status
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:map from = 'vcenter_address,esxi_host,esxi_hba_name,esxi_disk_device_id' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:enrich dict = 'temp-san_switch_fabric_nameserver_inventory' & src_key_cols = 'esxi_hba_wwpn' & dict_key_cols = 'hba_wwpn' & enrich_cols = 'switch_name,switch_ip,switch_port_name,switch_port_index,switch_chassis_wwn' & enrich_cols_as = 'switch_name,switch_ip,switch_port_name,switch_port_index,switch_chassis_node_id' &  return_empty_dict = 'yes' & return_empty_cols = 'yes'
       --> @dm:add-missing-columns columns = 'hba_wwpn'
       --> @dm:fixnull columns = 'hba_wwpn,switch_name,switch_ip,switch_port_name,switch_port_index,switch_chassis_node_id' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:selectcolumns exclude = '^hba_wwpn$'
       --> @dm:eval switch_port_index = "int(switch_port_index) if switch_port_index and switch_port_index != 'Not Available' else switch_port_index"
       --> @dm:save name = 'temp-processed-storage-devices-inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## Process ESXi Host FC HBA Configuration to capture WWN address for Topology
--> @c:new-block
    --> @dm:recall name = 'temp-processed-storage-devices-inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter esxi_hba_wwpn is not empty get esxi_host as 'asset_name',vcenter_address as 'inventory_source',esxi_hba_wwpn as 'endpoint_address',esxi_hba_name as 'interface_name',datacenter,collection_timestamp
       --> @dm:eval asset_type = "'Hypervisor'" & endpoint_address_type = "'WWN'"
       --> @dm:map attr = 'endpoint_address' & func = 'strip'
       --> @dm:map from = 'inventory_source,datacenter,asset_name' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'inventory_source,interface_name,endpoint_address_type,endpoint_address,interface_name' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:selectcolumns exclude = "^vcenter_address$|^datacenter$"
       --> @dm:fixnull-regex columns = '.*' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-esxi_host_fc_wwn_processed'
       --> @rn:write-stream name = 'network-endpoints-identity-stream'
    --> @exec:end-if

## VMware vCenter vSwitch Inventory - Data processing
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_vswitches_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'vSwitch'
       --> *dm:safe-filter * get vcenter_address,
               datacenter_name as 'datacenter',
               cluster_name as 'esxi_cluster',
               host_name as 'esxi_host',
               vswitch_name,
               type as 'vswitch_type',
               portgroup as 'vswitch_portgroup',
               pnic as 'vswitch_pnic',
               nicOrder_activeNic as 'vswitch_acitve_pnic',
               nicOrder_standbyNic as 'vswitch_standby_pnic',
               nicTeaming_notifySwitches as 'vswitch_nicteam_notify_switches',
               nicTeaming_policy as 'vswitch_nicteam_policy',
               nicTeaming_reversePolicy as 'vswitch_nicteam_reverse_policy',
               securityPolicy_allowPromiscuous as 'vswitch_promiscuous_policy',
               securityPolicy_forgedTransmits as 'vswitch_forgedtransmits_policy',
               securityPolicy_macChanges as 'vswitch_macchanges_policy',
               uplinks as 'vswitch_uplink',
               vswitch_network_io_control_enabled as 'vswitch_network_io_control_policy',
               vswitch_product_version as 'vswitch_version',
               cdp_address as 'vswitch_cdp_dev_ip',
               cdp_devId as 'vswitch_cdp_dev_name',
               cdp_hardwarePlatform as 'vswitch_cdp_dev_platform',
               cdp_mgmtAddr as 'vswitch_cdp_dev_mgmt_ip',
               cdp_portId as 'vswitch_cdp_dev_portid',
               lldp_chassisId as 'vswitch_lldp_dev_name',
               lldp_portId as 'vswitch_lldp_dev_portid',
               asset_object,
               collection_timestamp,
               asset_status
       --> @dm:map attr = 'vswitch_network_io_control_policy' & func = 'lower'
       --> @dm:eval phy_switch_name = "''"
       --> @dm:eval phy_switch_name = "vswitch_cdp_dev_name if vswitch_cdp_dev_name != '' else vswitch_lldp_dev_name if vswitch_lldp_dev_name != '' else phy_switch_name"
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:map attr='vswitch_portgroup' & func = 'strip'
       --> @dm:map from = 'vcenter_address,esxi_host,vswitch_name,vswitch_type,vswitch_portgroup,vswitch_pnic' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-processed-vswitch-inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## #####################################
## VMware vCenter VMkernel NIC Inventory
## VMware vCenter ESXi Host VMkernel Networks Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-vmware_host_networks_inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map to = 'asset_object' & func = 'fixed' & value = 'HostNetwork'
       --> *dm:safe-filter * get vcenter_address,
               datacenter,
               cluster as 'esxi_cluster',
               host as 'esxi_host',
               device as 'esxi_vmk_name',
               dvPort_portgroup as 'dvswitch_portgroup',
               ipaddress as 'esxi_vmk_ip_address',
               mac as 'esxi_vmk_mac',
               mtu as 'esxi_vmk_mtu',
               portgroup as 'vswitch_portgroup',
               subnetMask as 'esxi_vmk_subnet_mask',
               management as 'esxi_vmk_mgmt_status',
               vSphereProvisioning as 'esxi_vmk_provisioning_status',
               vSphereReplication as 'esxi_vmk_replication_status',
               vSphereReplicationNFC as 'esxi_vmk_replicationnfc_status',
               vmotion as 'esxi_vmk_vmotion_status',
               vsan as 'esxi_vmk_vsan_status',
               vsanWitness as 'esxi_vmk_vsan_witness_status',
               asset_object,
               collection_timestamp,
               asset_status
       --> @dm:enrich dict = 'temp-processed-vcenter-inventory' & src_key_cols = 'vcenter_address' & dict_key_cols = 'vcenter_address' & enrich_cols = 'vcenter_name,vcenter_os_type,vcenter_full_version,vcenter_uuid'
       --> @dm:enrich dict = 'temp-processed-vswitch-inventory' & src_key_cols = 'esxi_host,vswitch_portgroup,vcenter_address' & dict_key_cols = 'esxi_host,vswitch_portgroup,vcenter_address' & enrich_cols = 'vswitch_name,vswitch_type' & enrich_cols_as = 'std_vswitch_name,std_vswitch_type'
       --> @dm:rename-columns std_vswitch_portgroup = 'vswitch_portgroup'
       --> @dm:enrich dict = 'temp-processed-vswitch-inventory' & src_key_cols = 'esxi_host,dvswitch_portgroup,vcenter_address' & dict_key_cols = 'esxi_host,vswitch_portgroup,vcenter_address' & enrich_cols = 'vswitch_name,vswitch_type' & enrich_cols_as = 'dv_vswitch_name,dv_vswitch_type'
       --> @dm:rename-columns dv_vswitch_portgroup = 'vswitch_portgroup'
       --> @dm:add-missing-columns columns = 'dv_vswitch_name,dv_vswitch_type,dv_vswitch_portgroup,std_vswitch_name,std_vswitch_type,std_vswitch_portgroup'
       --> @dm:fixnull columns = 'dv_vswitch_name,dv_vswitch_type,dv_vswitch_portgroup,std_vswitch_name,std_vswitch_type,std_vswitch_portgroup'
       --> @dm:eval vswitch_name = "''"
       --> @dm:eval vswitch_type = "''"
       --> @dm:eval vswitch_portgroup = "''"
       --> @dm:eval vswitch_name = "dv_vswitch_name if dv_vswitch_name != '' else std_vswitch_name if std_vswitch_name != '' else vswitch_name"
       --> @dm:eval vswitch_type = "dv_vswitch_type if dv_vswitch_type != '' else std_vswitch_type if std_vswitch_type != '' else vswitch_type"
       --> @dm:eval vswitch_portgroup = "dv_vswitch_portgroup if dv_vswitch_portgroup != '' else std_vswitch_portgroup if std_vswitch_portgroup != '' else vswitch_portgroup"
       --> @dm:selectcolumns exclude = '^dv_vswitch_name$|^dv_vswitch_type$|^dv_vswitch_portgroup$|^std_vswitch_name$|^std_vswitch_type$|^std_vswitch_portgroup$'
       --> @dm:map from = 'vcenter_address,esxi_host,esxi_vmk_name' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-processed-esxi-host-vmk-inventory'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

## Process ESXi Host VMKNIC Network Configuration to capture MAC address and IP Address for Topology
--> @c:new-block
    --> @dm:recall name = 'temp-processed-esxi-host-vmk-inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter esxi_vmk_mac is not empty get esxi_host as 'asset_name',vcenter_address as 'inventory_source',esxi_vmk_mac as 'endpoint_address',esxi_vmk_ip_address as 'ip_address',esxi_vmk_name as 'interface_name',datacenter,collection_timestamp
       --> @dm:eval asset_type = "'Hypervisor'" & endpoint_address_type = "'MAC'"
       --> @dm:map attr = 'endpoint_address' & func = 'strip'
       --> @dm:map from = 'inventory_source,datacenter,asset_name' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'inventory_source,interface_name,endpoint_address_type,endpoint_address,ip_address' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:selectcolumns exclude = "^vcenter_address$|^datacenter$"
       --> @dm:fixnull-regex columns = '.*' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-esxi_host_network_mac_ip_processed'
       --> @rn:write-stream name = 'network-endpoints-identity-stream'
    --> @exec:end-if

## VMware vCenter Physical Switches connected to vSwtich or dvSwitch
--> @c:data-loop dataset='temp-variable-dataset' & columns = 'vcenter_src_name'
    --> @dm:recall name = 'temp-processed-vswitch-inventory' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:filter asset_object = 'vSwitch'
       --> @dm:fixnull columns = 'vswitch_cdp_dev_name,vswitch_lldp_dev_name,vswitch_cdp_dev_ip,vswitch_cdp_dev_mgmt_ip,vswitch_cdp_dev_portid,vswitch_lldp_dev_portid'
       --> @dm:eval phy_switch_name = "''"
       --> @dm:eval phy_switch_name = "vswitch_cdp_dev_name if vswitch_cdp_dev_name != '' else vswitch_lldp_dev_name if vswitch_lldp_dev_name != '' else phy_switch_name"
       --> @dm:eval phy_switch_ip = "''"
       --> @dm:eval phy_switch_ip = "vswitch_cdp_dev_mgmt_ip if vswitch_cdp_dev_mgmt_ip != '' else vswitch_cdp_dev_ip if vswitch_cdp_dev_ip != '' else phy_switch_ip"
       --> @dm:eval phy_switch_port = "''"
       --> @dm:eval phy_switch_port = "vswitch_cdp_dev_portid if vswitch_cdp_dev_portid != '' else vswitch_lldp_dev_portid if vswitch_lldp_dev_portid != '' else phy_switch_port"
       --> *dm:filter phy_switch_name is not empty
       --> @dm:grok column = 'phy_switch_name' & pattern = "%{DATA:switch_name_temp}\(%{DATA:phy_switch_serial}\)"
       --> @dm:add-missing-columns columns = 'switch_name_temp,phy_switch_serial'
       --> @dm:fixnull columns = 'switch_name_temp'
       --> @dm:eval phy_switch_name = "switch_name_temp if switch_name_temp != '' else phy_switch_name"
       --> @dm:selectcolumns exclude = '^switch_name_temp$'
       --> *dm:filter * get phy_switch_name,phy_switch_ip,vswitch_cdp_dev_platform as 'phy_switch_model',phy_switch_port,phy_switch_serial,esxi_host,vswitch_name,vswitch_type,datacenter,vcenter_address,vcenter_name,vswitch_pnic,collection_timestamp,asset_status
       --> @dm:eval asset_object = "'Switch'"
       --> @dm:map from = 'vcenter_name,vswitch_name,vswitch_pnic,phy_switch_port,phy_switch_name,asset_object' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-processed-vcenter-phy-switch-nodes'
       --> @rn:write-stream name = 'vmware-vcenter-inventory'
    --> @exec:end-if

