%% stream = no and limit = 0

%% import_source = "linux"

## Linux Server OS Inventory Collection
@c:new-block
    --> @dm:empty
    --> @dm:addrow linux_src_name='linux'
    --> @dm:save name = 'temp-linux_inventory_source_info'

## Linux Server OS Inventory IP List for Data Collection
--> @c:new-block
    --> @dm:recall name = 'asset_discovery_master_list'
    --> @dm:map attr = 'ip_address' & func = 'strip'
    --> @dm:map attr = 'type' & func = 'strip'
    --> @dm:map attr = 'port' & func = 'strip'
    --> @dm:map attr = 'type' & func = 'lower'
    --> @dm:map attr = 'discovery_scope' & func = 'strip'
    --> @dm:map attr = 'discovery_scope' & func = 'lower'
    --> *dm:filter type contains 'linux' & discovery_scope = 'yes'
    --> @dm:save name = 'temp-linux_inventory_ip_list'

## ##
## Linux OS System Inventory
--> @c:data-loop name = "linux-system-details" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:system-info column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'system-info'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_system_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'system-info'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Disk Inventory
--> @c:data-loop name = "linux-disk-details" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:disks column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'disks'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_disks_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'disks'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Disk FileSystem Inventory
--> @c:data-loop name = "linux-disk-fs-details" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:disk-usage column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'disk-usage'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_disk_usage_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'disk-usage'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Network Inventory
--> @c:data-loop name = "linux-network-details" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:network-config column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'network-config'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_network_config_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'network-config'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Software Packages Inventory
--> @c:data-loop name = "linux-software-packages" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:software-packages column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'software-packages'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_software_packages_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'software-packages'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Processes Inventory
--> @c:data-loop name = "linux-os-processes" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:processes column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'processes'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_processes_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'processes'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Services Inventory
--> @c:data-loop name = "linux-os-services" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:services column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'services'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_services_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'services'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## ##
## Linux OS Netstat Inventory
--> @c:data-loop name = "linux-os-services" & dataset = 'temp-linux_inventory_source_info' & columns = 'linux_src_name'
    --> @dm:recall name = 'temp-linux_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$linux_src_name:netstat column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = '600' & cli_timeout = '150'
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'netstat'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-linux_os_netstat_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$linux_src_name'"
       --> @dm:eval bot_source_type = "'linux_os'"
       --> @dm:eval bot_name = "'netstat'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Linux OS Inventory Data Processing
## Process Linux OS System Inventory Data
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_system_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter * get hostname,source_ip as 'host_os_ip',Operating_System as 'host_os_temp',rhel_release,Kernel as 'host_os_kernel',Architecture as 'host_architecture',CPUs as 'host_cpus',Cores_per_socket as 'host_cores_per_socket',Sockets as 'host_cpu_sockets',Threads_per_core as 'host_threads_per_core',Vendor_ID as 'host_cpu_vendor',Model_name as 'host_cpu_model',MemTotalkB as 'host_memory_gb',Chassis as 'host_machine_type',Family as 'host_machine_family',Hypervisor_vendor as 'host_hyperv_vendor',Machine_ID as 'host_machine_id',UUID as 'host_bios_uuid',Manufacturer as 'host_machine_vendor',Product_Name as 'host_machine_model',Serial_Number as 'host_machine_serial',Virtualization as 'host_virtualization',collection_status,collection_timestamp,collection_duration,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:map attr = 'host_bios_uuid' & func = 'lower'
       --> @dm:add-missing-columns columns = 'host_os_machine_serial,host_machine_model' and value = ''
       --> @dm:eval host_machine_type = "'VM' if 'VMware' in host_machine_model or 'Virt' in host_machine_model or 'VMware' in host_hyperv_vendor or 'Microsoft' in host_hyperv_vendor or 'Xen' in host_hyperv_vendor or 'Xen' in host_machine_vendor or 'VMware' in host_os_machine_serial else 'Baremetal' "
       --> @dm:eval host_machine_vendor = "'VMware_vSphere' if 'VMware' in host_machine_vendor or 'VMware' in host_os_machine_serial or 'VMware' in host_hyperv_vendor else 'Microsoft_Hyper-V' if 'Microsoft' in host_machine_vendor else 'Xen_Hypervisor' if 'Xen' in host_machine_vendor else 'HPE' if host_machine_vendor == 'HP' else 'Huawei' if 'Huawei' in host_machine_vendor else 'Unknown' if host_machine_vendor == '' else host_machine_vendor"
       --> @dm:eval host_os_version = "host_os_temp if rhel_release == '' else rhel_release"
       --> @dm:eval host_memory_gb = "round(int(host_memory_gb) / 1024 / 1024, 0) if host_memory_gb else host_memory_gb"
       --> @dm:to-type columns = 'host_memory_gb,host_cpus,host_cores_per_socket,host_cpu_sockets,host_threads_per_core' & type = 'int'
       --> @dm:selectcolumns exclude = '^rhel_release$|^host_os_temp$'
       --> @dm:eval host_os_vendor = "'Redhat' if 'Red Hat' in host_os_version else 'Suse' if 'SUSE' in host_os_version else 'Debian' if 'Debian' in host_os_version else 'Ubuntu' if 'Ubuntu' in host_os_version else 'CentOS' if 'CentOS' in host_os_version else 'Rocky' if 'Rocky' in host_os_version else 'Unknown'"
       --> @dm:add-missing-columns columns = 'host_os_machine_serial,host_machine_serial'
       --> @dm:fixnull columns = 'host_machine_serial,host_os_machine_serial'
       --> @dm:map to = 'host_machine_serial' & func = 'evaluate' & expr = "host_os_machine_serial if host_machine_serial == '' and host_os_machine_serial != '' else host_machine_serial"
       --> @dm:map from = 'host_bios_uuid' & to = 'host_bios_uuid_endian'
       --> @dm:map attr = 'host_bios_uuid_endian' & func = 'reverse_uuid_endian'
       --> @dm:grok column = 'host_machine_serial' & pattern = "VMware-%{GREEDYDATA:vm_serial}|%{GREEDYDATA:vm_serial}"
       --> @dm:add-missing-columns columns = 'vm_serial'
       --> @dm:fixnull columns = 'vm_serial'
       --> @dm:eval vm_serial_short = "vm_serial[:11] if vm_serial != '' else None" & uuid_short = "host_bios_uuid_endian[:8]"
       --> @dm:map attr = 'vm_serial_short' & func = 'replace' & oldvalue = ' ' & newvalue = ''
       --> @dm:map to = 'host_bios_uuid' & func = 'evaluate' & expr = "host_bios_uuid_endian if vm_serial_short == uuid_short else host_bios_uuid"
       --> @dm:selectcolumns exclude = '^host_bios_uuid_endian$|^vm_serial_short$|^uuid_short$|^vm_serial$|^meta_grok_message$'
       --> @dm:map attr = 'hostname' & func = 'strip'
       --> @dm:map attr = 'host_os_version' & func = 'strip'
       --> @dm:eval asset_object = "'System'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_bios_uuid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_system_processed_inventory'
    --> @exec:end-if

## Host OS Inventory - Virtual Machine mapping Enrichment
--> @c:new-block
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'VirtualMachine' with-input name = 'vmware-vcenter-inventory' & limit = 0 & skip_error = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter * get vm_name,vm_power_state,vm_ip_address,vm_additional_ips,vm_bios_uuid,vm_instance_uuid,esxi_host,esxi_cluster,datacenter,datastore_name,vswitch_portgroup,vcenter_name,vcenter_address
       --> @dm:save name = 'temp-vcenter-vm-nodes-dict'
       --> *exec:if-shape num_rows > 0
          --> @dm:recall name = 'temp-linux_system_processed_inventory' & return_empty = 'yes'
          --> *dm:safe-filter collection_status = 'Success'
          --> @dm:enrich dict = 'temp-vcenter-vm-nodes-dict' & src_key_cols = 'host_bios_uuid' & dict_key_cols = 'vm_bios_uuid' & enrich_cols = 'vm_name,vm_power_state,vm_ip_address,vm_additional_ips,vm_instance_uuid,esxi_host,esxi_cluster,datacenter,datastore_name,vswitch_portgroup,vcenter_name,vcenter_address'
          --> @dm:save name = 'temp-linux_os_system_inventory_enrich_dict'
          --> @dm:save name = 'linux_os_system_inventory_enrich_dict'
          --> @rn:write-stream name = 'host-os-inventory'
       --> @exec:end-if
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:recall name = 'temp-linux_system_processed_inventory' & return_empty = 'yes'
       --> @dm:save name = 'linux_os_system_inventory_enrich_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Linux OS Disks Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_disks_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter (TYPE = 'disk' and NAME != 'fd0') or MODEL contains 'disk' get source_ip as 'host_os_ip',NAME as 'host_os_disk_name',SIZE as 'host_os_disk_size',STATE as 'host_os_disk_state',MODEL as 'host_os_disk_model',SERIAL as 'host_os_disk_serial',UUID as 'host_os_disk_uuid',WWN as 'host_os_disk_wwn',VENDOR as 'host_os_disk_vendor',MOUNTPOINT as 'host_os_disk_mount',LABEL as 'host_os_disk_label',FSTYPE as 'host_os_disk_fstype',collection_status,collection_timestamp,collection_duration,reason
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:map attr = 'host_os_disk_wwn' & func = 'lower'
       --> @dm:eval host_os_disk_size = "int(host_os_disk_size) if host_os_disk_size else host_os_disk_size"
       --> @dm:eval host_os_disk_size_gb = "round(int(host_os_disk_size) / 1024 / 1024 / 1024, 0)"
       --> @dm:to-type columns = 'host_os_disk_size_gb,host_os_disk_size' & type = 'int'
       --> @dm:grok column = 'host_os_disk_wwn' & pattern = "0x%{GREEDYDATA:host_os_disk_wwn_temp}"
       --> @dm:add-missing-columns columns = 'host_os_disk_wwn_temp'
       --> @dm:eval disk_serial_length = "len(host_os_disk_serial) if host_os_disk_serial else 0" & disk_wwn_length = "len(host_os_disk_wwn_temp) if host_os_disk_wwn_temp else 0"
       --> @dm:eval lun_naa_id = "'naa.'+host_os_disk_serial if disk_serial_length == 32 else 'naa.'+host_os_disk_wwn_temp if disk_wwn_length == 32 else None"
       --> @dm:selectcolumns exclude = '^disk_serial_length$|^host_os_disk_wwn_temp$|^disk_wwn_length$'
       --> @dm:eval asset_object = "'Disk'"
       --> @dm:add-missing-columns columns = "asset_status" & value="Active"
       --> @dm:map from = 'hostname,host_os_ip,host_os_disk_name,host_os_disk_uuid,host_os_disk_serial' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_os_disks_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Linux OS Disk FileSystem Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_disk_usage_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter type does not contain 'tmpfs' and type does not contain 'squashfs' get source_ip as 'host_os_ip',filesystem as 'host_os_disk_volume',mounted_on as 'host_os_disk_mount',type as 'host_os_fs_type',size as 'host_os_fs_size',used as 'host_os_fs_used',available as 'host_os_fs_available',use_percent as 'host_os_fs_used_perc',collection_timestamp,collection_status,collection_duration,reason
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:grok column = 'host_os_fs_size' & pattern = "%{NUMBER:host_os_fs_size_temp}"
       --> @dm:grok column = 'host_os_fs_available' & pattern = "%{NUMBER:host_os_fs_available_temp}"
       --> @dm:grok column = 'host_os_fs_used' & pattern = "%{NUMBER:host_os_fs_used_temp}"
       --> @dm:to-type columns = 'host_os_fs_size_temp,host_os_fs_available_temp,host_os_fs_used_temp' & type = 'float'
       --> @dm:to-type columns = 'host_os_fs_size,host_os_fs_available,host_os_fs_used' & type = 'str'
       --> @dm:eval host_os_fs_size_bytes = "float(host_os_fs_size_temp) * 1024**4 if 'T' in host_os_fs_size else float(host_os_fs_size_temp) * 1024**3 if 'G' in host_os_fs_size else float(host_os_fs_size_temp) * 1024**2 if 'M' in host_os_fs_size else float(host_os_fs_size_temp) * 1024 if 'K' in host_os_fs_size else host_os_fs_size_temp"
       --> @dm:eval host_os_fs_available_bytes = "float(host_os_fs_available_temp) * 1024**4 if 'T' in host_os_fs_available else float(host_os_fs_available_temp) * 1024**3 if 'G' in host_os_fs_available else float(host_os_fs_available_temp) * 1024**2 if 'M' in host_os_fs_available else float(host_os_fs_available_temp) * 1024 if 'K' in host_os_fs_available else host_os_fs_available_temp"
       --> @dm:eval host_os_fs_used_bytes = "float(host_os_fs_used_temp) * 1024**4 if 'T' in host_os_fs_used else float(host_os_fs_used_temp) * 1024**3 if 'G' in host_os_fs_used else float(host_os_fs_used_temp) * 1024**2 if 'M' in host_os_fs_used else float(host_os_fs_used_temp) * 1024 if 'K' in host_os_fs_used else host_os_fs_used_temp"
       --> @dm:eval host_os_fs_size_gb = "round(host_os_fs_size_bytes / 1024 / 1024 / 1024, 2)"
       --> @dm:eval host_os_fs_used_gb = "round(host_os_fs_used_bytes / 1024 / 1024 / 1024, 2)"
       --> @dm:eval host_os_fs_available_gb = "round(host_os_fs_available_bytes / 1024 / 1024 / 1024, 2)"
       --> @dm:eval asset_object = "'Filesystem'"
       --> @dm:add-missing-columns columns = "asset_status" & value="Active"
       --> @dm:map from = 'hostname,host_os_ip,host_os_disk_mount' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_os_disk_usage_processed'
       --> @dm:to-type columns = 'host_os_fs_used_perc' & type = 'int'
       --> @dm:selectcolumns exclude = '^host_os_fs_size_bytes$|^host_os_fs_available_bytes$|^host_os_fs_used_bytes$|^host_os_fs_used_temp$|^host_os_fs_size_temp$|^host_os_fs_available_temp$|^meta_grok_message$'
       --> @dm:save name = 'temp-linux_os_disk_file_system_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Linux OS Network Configuration
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_network_config_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:fixcolumns
       --> *dm:safe-filter if_name != 'lo' and if_name does not contain 'veth' get source_ip as 'host_os_ip',if_name as 'host_os_nic_name',link_ether as 'host_os_nic_mac',inet as 'host_os_nic_ip',mtu as 'host_os_nic_mtu',state as 'host_os_nic_status',collection_timestamp,collection_status,collection_duration,reason
       --> @dm:explode column = 'host_os_nic_ip'
       --> @dm:map-multi-proc from = 'host_os_nic_ip' & to = 'host_os_nic_ip_orig' & _max_procs = 0
       --> @dm:map-multi-proc attr = 'host_os_nic_ip' & func = 'split' & sep = '/' & _max_procs = 0
       --> @dm:map-multi-proc attr = 'host_os_nic_ip' & func = 'slice' & toIdx = 1 & _max_procs = 0
       --> @dm:map-multi-proc attr = 'host_os_nic_ip' & func = 'join' & _max_procs = 0
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Network'"
       --> @dm:add-missing-columns columns = "asset_status" & value="Active"
       --> @dm:map from = 'hostname,host_os_ip,host_os_nic_mac,host_os_nic_name,host_os_nic_ip' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_os_network_config_processed'
       --> @dm:save name = 'linux_os_network_inventory_enrich_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Linux OS Network Configuration to capture MAC address and IP Address for Topology
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_network_config_processed' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter host_os_nic_mac is not empty get hostname as 'asset_name',host_os_ip as 'inventory_source',host_os_nic_mac as 'endpoint_address',host_os_nic_ip as 'ip_address',host_os_nic_name as 'interface_name',host_bios_uuid,collection_timestamp
       --> @dm:eval asset_type = "'Host_OS'" & endpoint_address_type = "'MAC'"
       --> @dm:map attr = 'endpoint_address' & func = 'strip'
       --> @dm:map from = 'asset_name,inventory_source,host_bios_uuid' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'inventory_source,interface_name,endpoint_address_type,endpoint_address,ip_address' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:selectcolumns exclude = "^host_bios_uuid$"
       --> @dm:fixnull-regex columns = '.*' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-linux_os_network_mac_ip_processed'
       --> @rn:write-stream name = 'network-endpoints-identity-stream'
    --> @exec:end-if

## Process Linux OS Software Packages Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_software_packages_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'source_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor'
       --> *exec:if-condition host_os_vendor = 'Ubuntu'
          --> @dm:selectcolumns exclude = '^host_os_ip$|^host_os_vendor$'
          --> *dm:safe-filter * get Package as 'host_os_app_name',Version as 'host_os_app_version',Provides as 'host_os_app_package',Description as 'host_os_app_description',Architecture as 'host_os_app_arch',Maintainer as 'host_os_app_vendor',Status as 'host_os_app_install_status',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
          --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr'
          --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
          --> @dm:eval asset_object = "'Software'"
          --> @dm:map to  = 'asset_status' & func = 'fixed' & value = 'Active'
          --> @dm:map from = 'hostname,host_os_ip,host_os_app_name,host_os_app_version' & to = 'unique_id' & func = 'join' & sep = '_'
          --> @dm:dedup columns = 'unique_id'
          --> @dm:save name = 'temp-linux_ubuntu_software_packages_processed_success'
          --> @rn:write-stream name = 'host-os-inventory'
       --> @exec:end-if
       --> *exec:if-condition host_os_vendor != 'Ubuntu'
          --> *dm:safe-filter * get Name as 'host_os_app_name',Version as 'host_os_app_version',Source_RPM as 'host_os_app_package',Description as 'host_os_app_description',Architecture as 'host_os_app_arch',Vendor as 'host_os_app_vendor',License as 'host_os_app_license',Group as 'host_os_app_group',Install_Date as 'host_os_app_install_date',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
          --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr'
          --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
          --> @dm:add-missing-columns columns = "asset_object" & value="Software"
          --> @dm:add-missing-columns columns = "asset_status" & value="Active"
          --> @dm:map from = 'hostname,host_os_ip,host_os_app_name,host_os_app_version' & to = 'unique_id' & func = 'join' & sep = '_'
          --> @dm:save name = 'temp-linux_software_packages_processed_success'
          --> *dm:safe-filter host_os_app_name is not empty and host_os_app_version is not empty
          --> @dm:dedup columns = 'unique_id'
          --> @dm:save name = 'temp-linux_software_packages_deduped_data'
          --> @rn:write-stream name = 'host-os-inventory'
       --> @exec:end-if
    --> @exec:end-if

## Process Linux OS Processes Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_processes_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter * get source_ip as 'host_os_ip',exe as 'host_os_process_name',pid as 'host_os_process_pid',ppid as 'host_os_process_ppid',collection_timestamp,collection_status,collection_duration,reason
       --> @dm:change-time-format columns = 'collection_timestamp' & from_format = 'ms' & to_format = 'datetimestr'
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Process'"
       --> @dm:add-missing-columns columns = "asset_status" & value="Active"
       --> @dm:map from = 'hostname,host_os_ip,host_os_process_name,host_os_process_pid,host_os_process_ppid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_os_processes_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Linux OS Services Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_services_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter * get source_ip as 'host_os_ip',description as 'host_os_service_name',unit as 'host_os_service_id',active as 'host_os_service_state',sub as 'host_os_service_status',MainPID as 'host_os_service_pid',collection_timestamp,collection_status,reason,collection_duration
       --> *dm:safe-filter host_os_service_status = 'running'
       --> @dm:change-time-format columns = 'collection_timestamp' & from_format = 'ms' & to_format = 'datetimestr'
       --> @dm:to-type columns = 'host_os_service_pid' & type = 'int'
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval host_os_service_name = "host_os_service_id if host_os_service_name == '' else host_os_service_name"
       --> @dm:eval host_os_service_state = "'active' if host_os_service_status == 'running' and host_os_service_state == '' else host_os_service_state"
       --> @dm:eval asset_object = "'Service'"
       --> @dm:add-missing-columns columns = "asset_status" & value="Active"
       --> @dm:map from = 'hostname,host_os_ip,host_os_service_name' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_os_services_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Collect and process Linux OS Netstat data
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_netstat_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter state does not contain '_'
       --> *dm:safe-filter pid is not empty
       --> @dm:dedup columns = 'local_address,pid,program_name,transport_protocol,foreign_address,foreign_port,state'
       --> *dm:safe-filter * get local_address as 'local_ip_address',local_port as 'local_port',foreign_address as 'remote_ip_address',foreign_port as 'remote_port',transport_protocol as 'connection_protocol',program_name as 'host_os_process_name',pid as 'host_os_process_pid',state as 'connection_state',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
       --> @dm:copy-columns from = 'local_ip_address,local_port,remote_ip_address,remote_port,connection_protocol' & to = 'local_ip_address,local_port,remote_ip_address,remote_port,connection_protocol' & func = 'strip'
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & _max_procs = 0
       --> @dm:enrich dict='temp-linux_system_processed_inventory' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:to-type columns = 'local_port,remote_port,host_os_process_pid' & type = 'int'
       --> @dm:save name = 'temp-linux_os_netstat_processed'
    --> @exec:end-if

## ### Application to Application Dependency ###########
## Step-01 - Establish Server Port
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_netstat_processed' & empty_as_null = 'no' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:fixnull columns = 'connection_state,local_ip_address,remote_ip_address,remote_port'
       --> @dm:copy-columns from = 'local_ip_address' & to = 'local_ip_address_orig'
       --> @dm:map-multi-proc to = "local_ip_address" & func = "evaluate" & expr = "host_os_ip if local_ip_address == '127.0.0.1' or local_ip_address == '0.0.0.0' or local_ip_address == '::' or local_ip_address == '::1' or local_ip_address != host_os_ip else local_ip_address" & _max_procs = 0
       --> *dm:safe-filter local_ip_address does not contain '::'
       --> *dm:safe-filter connection_state contain "LISTEN" or (connection_state == "" and (remote_port == "*" or remote_port == ""))
       --> @dm:dedup columns = 'host_os_ip,local_port,connection_protocol'
       --> @dm:map-multi-proc to = "server_port_check" & func = "evaluate" & expr = "'Yes' if local_port != '0' else 'No'" & _max_procs = 0
       --> @dm:selectcolumns exclude = "^remote_.*$"
       --> @dm:eval asset_object = "'NetstatServerPorts'"
       --> @dm:eval asset_status = "'Active'"
       --> @dm:to-type columns = 'local_port,host_os_process_pid' & type = 'int'
       --> @dm:map from = 'host_os_ip,local_ip_address,local_port,host_os_process_name,host_os_process_pid,connection_protocol' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-linux_netstat_server_port_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Step-02 - Establish Client - Server Relationship
--> @c:new-block
    --> @dm:recall name = "temp-linux_os_netstat_processed" & empty_as_null = 'no' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:fixnull columns = 'connection_state,local_ip_address,remote_ip_address,remote_port'
       --> *dm:safe-filter connection_state does not contain "LISTEN" and (connection_state != "" or remote_port != "*") and local_port != remote_port and local_ip_address != '127.0.0.1' and remote_ip_address != '127.0.0.1' and remote_port != ""
       --> @dm:copy-columns from = 'local_ip_address' & to = 'local_ip_address_orig'
       --> @dm:copy-columns from = 'remote_ip_address' & to = 'remote_ip_address_orig'
       --> @dm:map-multi-proc to = "local_ip_address" & func = "evaluate" & expr = "host_os_ip if local_ip_address == '127.0.0.1' or local_ip_address == '0.0.0.0' or local_ip_address != host_os_ip or local_ip_address == '::1' else local_ip_address" & _max_procs = 0
       --> @dm:map-multi-proc to = "remote_ip_address" & func = "evaluate" & expr = "host_os_ip if remote_ip_address == '127.0.0.1' or remote_ip_address == '0.0.0.0' or local_ip_address == remote_ip_address or remote_ip_address == '::1' else remote_ip_address" & _max_procs = 0
       --> @dm:enrich dict = 'temp-linux_netstat_server_port_dict' & src_key_cols = 'local_ip_address,local_port,connection_protocol' & dict_key_cols = 'local_ip_address,local_port,connection_protocol' & enrich_cols = 'server_port_check'
       --> @dm:map-multi-proc to = "server_port_check" & func = "evaluate" & expr = "'No' if server_port_check != 'Yes' else server_port_check" & _max_procs = 0
       --> @dm:map-multi-proc to = "port_type" & func = "evaluate" & expr = "'SERVER' if server_port_check == 'Yes' else 'CLIENT'" & _max_procs = 0
       --> @dm:map-multi-proc to = "client_ip_address" & func = "evaluate" & expr = "local_ip_address if port_type == 'CLIENT' else remote_ip_address" & _max_procs = 0
       --> @dm:map-multi-proc to = "client_port" & func = "evaluate" & expr = "local_port if port_type == 'CLIENT' else remote_port" & _max_procs = 0
       --> @dm:map-multi-proc to = "server_ip_address" & func = "evaluate" & expr = "local_ip_address if port_type == 'SERVER' else remote_ip_address" & _max_procs = 0
       --> @dm:map-multi-proc to = "server_port" & func = "evaluate" & expr = "local_port if port_type == 'SERVER' else remote_port" & _max_procs = 0
       --> @dm:map-multi-proc to = "connection_flow" & func = "evaluate" & expr = "'local_to_local' if client_ip_address == server_ip_address else 'local_to_remote' if port_type == 'CLIENT' and client_ip_address != server_ip_address else 'remote_to_local' if port_type == 'SERVER' and client_ip_address != server_ip_address else 'Unknown'" & _max_procs = 0
       --> @dm:dedup columns = 'client_ip_address,client_port,server_ip_address,server_port'
       --> @dm:selectcolumns exclude = '^local_ip_address.*$|^local_port$|^remote_ip_address.*$|^remote_port$|^unique_id$|^port_type$|^server_port_check$'
       --> @dm:eval asset_object = "'NetstatClientServer'"
       --> @dm:eval asset_status = "'Active'"
       --> @dm:to-type columns = 'client_port,server_port' & type = 'int'
       --> @dm:map from = 'client_ip_address,client_port,server_ip_address,server_port,connection_state,connection_protocol' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-linux_netstat_client_server_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Host OS Topology Nodes + VMware Virtual Machine Node ID Lookup Enrichment
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-vm-nodes-dict' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:recall name = 'temp-linux_system_processed_inventory' & return_empty = 'yes'
       --> *dm:safe-filter collection_status = 'Success'
       --> @dm:enrich dict = 'temp-vcenter-vm-nodes-dict' & src_key_cols = 'host_bios_uuid' & dict_key_cols = 'vm_bios_uuid' & enrich_cols = 'vcenter_address,vm_name,vm_instance_uuid'
       --> @dm:map from = 'vcenter_address,vm_name,vm_instance_uuid' & to = 'vm_node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'vm_name' & to = 'vm_node_label'
       --> @dm:selectcolumns exclude = '^vcenter_address$|^vm_name$|^vm_instance_uuid$'
       --> @dm:eval layer = "'Compute'"
       --> @dm:eval node_type = "'Host_OS'"
       --> @dm:eval iconURL = "'Host'"
       --> @dm:rename-columns node_id = 'unique_id'
       --> @dm:map from = 'hostname' & to = 'node_label'
       --> @dm:save name = 'temp-linux_system_vm_info_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:recall name = 'temp-linux_system_processed_inventory' & return_empty = 'yes'
       --> *dm:safe-filter collection_status = 'Success'
       --> @dm:eval layer = "'Compute'"
       --> @dm:eval node_type = "'Host_OS'"
       --> @dm:eval iconURL = "'Host'"
       --> @dm:rename-columns node_id = 'unique_id'
       --> @dm:map from = 'hostname' & to = 'node_label'
       --> @dm:save name = 'temp-linux_system_vm_info_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## Host OS Services Topology Nodes
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_services_processed' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'host_os_service_status' & func = 'strip'
       --> *dm:safe-filter collection_status = 'Success' and host_os_service_status = 'running'
       --> @dm:eval layer = "'Application Component'"
       --> @dm:eval node_type = "'OS_Service'"
       --> @dm:eval iconURL = "'Service'"
       --> @dm:rename-columns node_id = 'unique_id'
       --> @dm:map from = 'hostname,host_os_service_name' & to = 'node_label' & func = 'join' & sep = '_'
       --> @dm:map from = 'hostname,host_os_ip,host_bios_uuid' & to = 'host_os_node_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-linux_os_services_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## Host OS Topology Edges
## Host OS Nodes to Services Relationship
--> @c:new-block
    --> @dm:recall name = 'temp-linux_os_services_nodes' & return_empty = 'yes'
    --> *dm:safe-filter host_os_node_id is not empty
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter * get host_os_node_id as 'left_id',hostname as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type'
       --> @dm:eval left_node_type = "'Host_OS'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'runs'
       --> @dm:save name = 'temp-host-os-to-services-edges'
    --> @exec:end-if

## Host OS Nodes to VMware VM Relationship
--> @c:new-block
    --> @dm:recall name = 'temp-linux_system_vm_info_nodes' & return_empty = 'yes'
    --> *dm:safe-filter vm_node_id is not empty
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter * get vm_node_id as 'left_id',vm_node_label as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type'
       --> @dm:eval left_node_type = "'VM'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'runs'
       --> @dm:save name = 'temp-host-os-to-vm-edges'
    --> @exec:end-if

## Generate Host OS Edges Ingestion ID
--> @c:new-block
    --> @dm:empty
    --> @dm:addrow inventory_source = 'linux_system_info'
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> @dm:map from = 'collection_timestamp' & to = 'topology_ingestion_id' & func = 'md5'
    --> @dm:save name = 'temp-topology-system-edges-ingestion-id-dict'

## Host OS Node Topology - Edges
--> @c:new-block
    --> @dm:recall name = 'temp-host-os-to-vm-edges' & return_empty = 'yes'
    --> @dm:skip-block-if-shape row_count = 0
    --> @dm:map attr = 'left_id' & func = 'strip'
    --> @dm:map attr = 'right_id' & func = 'strip'
    --> @dm:map from = 'left_id' & to = 'left'
    --> @dm:map from = 'right_id' & to = 'right'
    --> @dm:eval inventory_source = "'linux_system_info'"
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> *dm:safe-filter left_id != right_id
    --> *dm:safe-filter left_label is not empty
    --> *dm:safe-filter right_label is not empty
    --> @dm:dedup columns = 'left_id,right_id'
    --> @dm:enrich dict = 'temp-topology-system-edges-ingestion-id-dict' & src_key_cols = 'inventory_source' & dict_key_cols = 'inventory_source' & enrich_cols = 'topology_ingestion_id'
    --> *dm:safe-filter * get left_label,left_id,left_node_type,relation_type,right_id,right_label,right_node_type,inventory_source,topology_ingestion_id,collection_timestamp
    --> @dm:save name = 'temp-host-os-nodes-topology-edges'
    --> @rn:write-stream name = 'cfx_rdaf_topology_edges'
    --> @graph:insert-edges dbname = 'cfx_rdaf_topology' & nodes_collection = 'cfx_rdaf_topology_nodes' & edges_collection = 'cfx_rdaf_topology_edges' & left_id = 'left_id' & right_id = 'right_id'

## Generate Host OS Services Edges Ingestion ID
--> @c:new-block
    --> @dm:empty
    --> @dm:addrow inventory_source = 'linux_services_info'
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> @dm:map from = 'collection_timestamp' & to = 'topology_ingestion_id' & func = 'md5'
    --> @dm:save name = 'temp-topology-service-edges-ingestion-id-dict'

## Host OS Node Topology - Edges
--> @c:new-block
    --> @dm:recall name = 'temp-host-os-to-services-edges' & return_empty = 'yes'
    --> @dm:skip-block-if-shape row_count = 0
    --> @dm:map attr = 'left_id' & func = 'strip'
    --> @dm:map attr = 'right_id' & func = 'strip'
    --> @dm:map from = 'left_id' & to = 'left'
    --> @dm:map from = 'right_id' & to = 'right'
    --> @dm:eval inventory_source = "'linux_services_info'"
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> *dm:safe-filter left_id != right_id
    --> *dm:safe-filter left_label is not empty
    --> *dm:safe-filter right_label is not empty
    --> @dm:dedup columns = 'left_id,right_id'
    --> @dm:enrich dict = 'temp-topology-service-edges-ingestion-id-dict' & src_key_cols = 'inventory_source' & dict_key_cols = 'inventory_source' & enrich_cols = 'topology_ingestion_id'
    --> *dm:safe-filter * get left_label,left_id,left_node_type,relation_type,right_id,right_label,right_node_type,inventory_source,topology_ingestion_id,collection_timestamp
    --> @dm:save name = 'temp-host-os-services-nodes-topology-edges'
    --> @rn:write-stream name = 'cfx_rdaf_topology_edges'
    --> @graph:insert-edges dbname = 'cfx_rdaf_topology' & nodes_collection = 'cfx_rdaf_topology_nodes' & edges_collection = 'cfx_rdaf_topology_edges' & left_id = 'left_id' & right_id = 'right_id'

## Delete Stale Linux OS Topology Edges
--> @c:new-block
    --> @dm:empty
    --> @dm:sleep seconds = 60

## Delete Stale Linux OS System Topology Edges from Pstream
--> @c:new-block
    --> @dm:recall name = 'temp-topology-system-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> #dm:pstream-delete-data-by-query topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}' with-input name = 'cfx_rdaf_topology_edges' & timeout = 300
          --> @dm:save name = 'temp-linux_system_deleted_edges'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale Linux OS System Topology Edges from GraphDB
--> @c:new-block
    --> @dm:recall name = 'temp-topology-system-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> @graph:delete-by-query dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_edges' & delete_query = "topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}'"
          --> @dm:save name = 'temp-linux_system_deleted_edges_from_graphdb'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale Linux OS Service Topology Edges from Pstream
--> @c:new-block
    --> @dm:recall name = 'temp-topology-service-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> #dm:pstream-delete-data-by-query topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}' with-input name = 'cfx_rdaf_topology_edges' & timeout = 300
          --> @dm:save name = 'temp-linux_service_deleted_edges'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale Linux OS Service Topology Edges from GraphDB
--> @c:new-block
    --> @dm:recall name = 'temp-topology-service-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> @graph:delete-by-query dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_edges' & delete_query = "topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}'"
          --> @dm:save name = 'temp-linux_service_deleted_edges_from_graphdb'
       --> @exec:end-loop
    --> @exec:end-if
