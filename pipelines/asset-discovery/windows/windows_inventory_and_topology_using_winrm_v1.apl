%% stream = no and limit = 0

%% import_source = "windows-inventory"

## Windows Server OS Inventory Collection
@c:new-block
    --> @dm:empty
    --> @dm:addrow windows_src_name = 'windows-inventory'
    --> @dm:save name = 'temp-windows_inventory_source_info'

## ##
## Windows Server OS Inventory IP List for Data Collection
--> @c:new-block
    --> @dm:recall name = 'asset_discovery_master_list'
    --> @dm:map attr = 'ip_address' & func = 'strip'
    --> @dm:map attr = 'type' & func = 'strip'
    --> @dm:map attr = 'port' & func = 'strip'
    --> @dm:map attr = 'type' & func = 'lower'
    --> @dm:map attr = 'discovery_scope' & func = 'strip'
    --> @dm:map attr = 'discovery_scope' & func = 'lower'
    --> *dm:filter type contains 'windows' & discovery_scope = 'yes'
    --> @dm:save name = 'temp-windows_inventory_ip_list'

## ##
## Windows OS System Inventory
--> @c:data-loop name = "windows-system-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:system-info column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'system-info'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_system_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'system-info'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS BIOS Inventory
--> @c:data-loop name = "windows-bios-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:bios-details column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'bios-details'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_bios_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'bios-details'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS Disks Inventory
--> @c:data-loop name = "windows-disk-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:disks column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'disks'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_disks_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'disks'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS Disk Filesystem Inventory
--> @c:data-loop name = "windows-disk-fs-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:disk-usage column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'disk-usage'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_disk_fs_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'disk-usage'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS Network Configuration Inventory
--> @c:data-loop name = "windows-network-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:network-config column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'network-config'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_network_config_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'network-config'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS Software Packages Inventory
--> @c:data-loop name = "windows-software-package-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:software-packages column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'software-packages'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_software_packages_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'software-packages'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS processes Inventory
--> @c:data-loop name = "windows-processes-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:processes column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'processes'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_processes_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'processes'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS services Inventory
--> @c:data-loop name = "windows-services-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:services column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'services'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_services_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'services'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS Netstat Inventory
--> @c:data-loop name = "windows-netstat-details" & dataset = 'temp-windows_inventory_source_info' & columns = 'windows_src_name'
    --> @dm:recall name = 'temp-windows_inventory_ip_list'
    --> *dm:safe-filter ip_address is not Empty
    --> @$windows_src_name:netstat column_name='ip_address' & concurrent_discovery = '100' & connect_timeout = "120" & cli_timeout = "100"
    --> *exec:if-condition collection_status != 'Success'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'netstat'"
       --> @dm:to-type columns = 'reason' & type = 'str'
       --> @dm:eval collection_timestamp = "collection_timestamp if collection_timestamp else time_now_as_isoformat()"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:save name = 'temp-windows_os_netstat_inventory' & append = 'yes'
       --> @dm:dedup columns = 'source_ip'
       --> *dm:safe-filter * get source_ip as 'asset_id',collection_status,collection_timestamp,reason
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval bot_source_name = "'$windows_src_name'"
       --> @dm:eval bot_source_type = "'windows_os'"
       --> @dm:eval bot_name = "'netstat'"
       --> @rn:write-stream name = 'asset_inventory_collection_status_stream'
    --> @exec:end-if

## Windows OS Inventory Data Processing
## Process Windows OS BIOS Details Inventory Data
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_bios_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter * get source_ip as 'host_os_ip',Vendor as 'host_os_machine_vendor',SerialNumber as 'host_os_machine_serial',UUID as 'host_bios_uuid',collection_status,collection_timestamp,collection_duration,reason
       --> @dm:add-missing-columns columns = 'host_os_machine_serial,host_hyperv_vendor,host_machine_model,host_machine_vendor' and value = ''
       --> @dm:eval host_machine_type = "'VM' if 'VMware' in host_machine_model or 'Virt' in host_machine_model or 'VMware' in host_hyperv_vendor or 'Microsoft' in host_hyperv_vendor or 'Xen' in host_hyperv_vendor or 'Xen' in host_machine_vendor or 'VMware' in host_os_machine_serial else 'Baremetal' "
       --> @dm:map attr = 'host_bios_uuid' & func = 'lower'
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:save name = 'temp-windows_bios_dict'
    --> @exec:end-if

## Process Windows OS System Inventory Data
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_system_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter * get Host_Name as 'hostname',source_ip as 'host_os_ip',OS_Name as 'host_os_version',OS_Version as 'host_os_kernel',Domain as 'host_os_domain',System_Type as 'host_architecture',CPU_NumberOfLogicalProcessors as 'host_cpus',CPU_NumberOfCores as 'host_cores_per_socket',CPU_Sockets as 'host_cpu_sockets',CPU_Manufacturer as 'host_cpu_vendor',CPU_Name as 'host_cpu_model',Total_Physical_Memory_MB as 'host_memory_gb',uuid as 'host_bios_uuid',System_Manufacturer as 'host_machine_vendor',System_Model as 'host_machine_model',BIOS_Version as 'host_os_bios_version',collection_status,collection_timestamp,collection_duration,reason
       --> @dm:map attr = 'host_bios_uuid' & func = 'lower'
       --> @dm:enrich dict = 'temp-windows_bios_dict' & src_key_cols = 'host_os_ip,host_bios_uuid' & dict_key_cols = 'host_os_ip,host_bios_uuid' & enrich_cols = 'host_os_machine_serial,host_machine_type'
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
       --> @dm:map attr = 'host_memory_gb' & func = 'replace' & oldvalue = '.' & newvalue = ''
       --> @dm:eval host_memory_gb = "round(int(host_memory_gb) / 1024, 0) if host_memory_gb else host_memory_gb"
       --> @dm:eval host_os_vendor = "'Microsoft'"
       --> @dm:add-missing-columns columns = 'host_hyperv_vendor' and value = ''
       --> @dm:fixnull columns = 'host_machine_vendor,host_os_machine_serial,host_hyperv_vendor'
       --> @dm:eval host_machine_vendor = "'VMware_vSphere' if 'VMware' in host_machine_vendor or 'VMware' in host_os_machine_serial or 'VMware' in host_hyperv_vendor else 'Microsoft_Hyper-V' if 'Microsoft' in host_machine_vendor else 'Xen_Hypervisor' if 'Xen' in host_machine_vendor else 'HPE' if host_machine_vendor == 'HP' else 'Huawei' if 'Huawei' in host_machine_vendor else 'Unknown' if host_machine_vendor == '' else host_machine_vendor"
       --> @dm:map attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms'
       --> @dm:eval asset_object = "'System'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_bios_uuid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-windows_host_system_info'
    --> @exec:end-if

## Host OS Inventory - Virtual Machine mapping Enrichment
--> @c:new-block
    --> @dm:empty
    --> #dm:query-persistent-stream asset_object = 'VirtualMachine' with-input name = 'vmware-vcenter-inventory' & limit = 0 & skip_error = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:filter * get vm_name,vm_power_state,vm_ip_address,vm_additional_ips,vm_bios_uuid,vm_instance_uuid,esxi_host,esxi_cluster,datacenter,datastore_name,vswitch_portgroup,vcenter_name,vcenter_address
       --> @dm:save name = 'temp-vcenter-vm-nodes-dict'
       --> *exec:if-shape num_rows > 0
          --> @dm:recall name = 'temp-windows_host_system_info'
          --> *dm:filter collection_status = 'Success'
          --> @dm:enrich dict = 'temp-vcenter-vm-nodes-dict' & src_key_cols = 'host_bios_uuid' & dict_key_cols = 'vm_bios_uuid' & enrich_cols = 'vm_name,vm_power_state,vm_ip_address,vm_additional_ips,vm_instance_uuid,esxi_host,esxi_cluster,datacenter,datastore_name,vswitch_portgroup,vcenter_name,vcenter_address'
          --> @dm:save name = 'windows_os_system_inventory_enrich_dict'
          --> @rn:write-stream name = 'host-os-inventory'
       --> @exec:end-if
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:recall name = 'temp-windows_host_system_info' & return_empty = 'yes'
       --> @dm:save name = 'windows_os_system_inventory_enrich_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows Disks Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_disks_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:fixcolumns
       --> *dm:safe-filter InterfaceType = 'SCSI' get source_ip as 'host_os_ip',Name as 'host_os_disk_name',Size as 'host_os_disk_size',Status as 'host_os_disk_state',Model as 'host_os_disk_model',SerialNumber as 'host_os_disk_serial',PNPDeviceID as 'host_os_disk_uuid',UniqueId as 'host_os_disk_wwn',collection_status,collection_timestamp,collection_duration,reason
       --> @dm:eval-multi-proc host_os_disk_vendor = "'VMware' if 'VMware' in host_os_disk_model else 'HPE' if '3PAR' in host_os_disk_model else 'Dell' if 'COMPELNT' in host_os_disk_model else 'Dell' if 'DELL' in host_os_disk_model else 'Dell EMC' if 'DGC' in host_os_disk_model else 'Hitachi' if 'HITACHI' in host_os_disk_model else 'NetApp' if 'NETAPP' in host_os_disk_model else 'HPE' if 'HP LOGICAL' in host_os_disk_model else 'IBM' if 'IBM' in host_os_disk_model else 'Microsoft' if 'MSFT' in host_os_disk_model else 'Microsoft' if 'Microsoft' in host_os_disk_model else 'Oracle' if 'Oracle' in host_os_disk_model else 'Unknown'" & _max_procs = 0
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:eval-multi-proc host_os_disk_size_gb = "round(host_os_disk_size / 1024 / 1024 / 1024, 0)" & _max_procs = 0
       --> @dm:to-type columns = 'host_os_disk_size_gb,host_os_disk_size' & type = 'int'
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:map attr = 'host_os_disk_wwn' & func = 'lower'
       --> @dm:eval lun_naa_id = "'naa.'+host_os_disk_wwn if host_os_disk_wwn else None"
       --> @dm:eval asset_object = "'Disk'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_os_disk_name,host_os_disk_uuid,host_os_disk_serial' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-windows_os_disks_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows Disks FileSystem Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_disk_fs_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:fixcolumns
       --> *dm:safe-filter * get source_ip as 'host_os_ip',Path as 'host_os_disk_volume', Name as 'host_os_disk_mount',Description as 'host_os_disk_type',FileSystem as 'host_os_fs_type',Size as 'host_os_fs_size',size_used as 'host_os_fs_used',FreeSpace as 'host_os_fs_available',used_percent as 'host_os_fs_used_perc',collection_timestamp,collection_status,collection_duration,reason
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:eval-multi-proc host_os_fs_size = "round(int(host_os_fs_size) / 1024 / 1024 / 1024, 0) if host_os_fs_size else host_os_fs_size" & _max_procs = 0
       --> @dm:eval-multi-proc host_os_fs_used = "round(int(host_os_fs_used) / 1024 / 1024 / 1024, 0) if host_os_fs_used else host_os_fs_used" & _max_procs = 0
       --> @dm:eval-multi-proc host_os_fs_available = "round(int(host_os_fs_available) / 1024 / 1024 / 1024, 0) if host_os_fs_available else host_os_fs_available" & _max_procs = 0
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Filesystem'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_os_disk_mount' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:to-type columns = 'host_os_fs_size,host_os_fs_used,host_os_fs_available,host_os_fs_used_perc' & type = 'int'
       --> @dm:save name = 'temp-windows_os_disk_fs_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows Network Configuration Inventory
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_network_config_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:fixcolumns
       --> *dm:safe-filter mac_address is not empty get source_ip as 'host_os_ip',description as 'host_os_nic_name',mac_address as 'host_os_nic_mac',ip_address as 'host_os_nic_ip',mtu as 'host_os_nic_mtu',net_connection_status as 'host_os_nic_status',collection_timestamp,collection_status,collection_duration,reason
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & _max_procs = 0
       --> @dm:explode column = 'host_os_nic_ip'
       --> *dm:filter host_os_nic_ip does not contain '::' and host_os_nic_name does not contain 'Miniport'
       --> @dm:map attr = 'host_os_nic_ip' & func = 'strip'
       --> @dm:map attr = 'host_os_nic_mac' & func = 'lower'
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Network'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_os_nic_mac,host_os_nic_name,host_os_nic_ip' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-windows_os_network_processed'
       --> @dm:save name = 'windows_os_network_inventory_enrich_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows OS Network Configuration to capture MAC address and IP Address for Topology
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_network_processed' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter host_os_nic_mac is not empty get hostname as 'asset_name',host_os_ip as 'inventory_source',host_os_nic_mac as 'endpoint_address',host_os_nic_ip as 'ip_address',host_os_nic_name as 'interface_name',host_bios_uuid,collection_timestamp
       --> @dm:eval asset_type = "'Host_OS'" & endpoint_address_type = "'MAC'"
       --> @dm:map attr = 'endpoint_address' & func = 'strip'
       --> @dm:map from = 'asset_name,inventory_source,host_bios_uuid' & to = 'node_id' & func = 'join' & sep = '_'
       --> @dm:map from = 'inventory_source,interface_name,endpoint_address_type,endpoint_address,ip_address' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:selectcolumns exclude = "^host_bios_uuid$"
       --> @dm:fixnull-regex columns = '.*' & value = 'Not Available' & apply_for_empty = 'yes'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-windows_os_network_mac_ip_processed'
       --> @rn:write-stream name = 'network-endpoints-identity-stream'
    --> @exec:end-if

## Process Windows Installed Applications
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_software_packages_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter DisplayName is not None get DisplayName as 'host_os_app_name',DisplayVersion as 'host_os_app_version',InstallDate as 'host_os_app_install_date',Publisher as 'host_os_app_vendor',InstallLocation as 'host_os_app_install_path',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Software'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_os_app_name,host_os_app_version' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-windows_os_software_packages_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows OS Processes
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_processes_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:fixcolumns
       --> *dm:safe-filter * get name as 'host_os_process_name',pid as 'host_os_process_pid',ppid as 'host_os_process_ppid',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:to-type columns = 'host_os_process_pid,host_os_process_ppid' & type = 'int'
       --> @dm:map-multi-proc attr = 'host_os_process_pid' & func = 'strip' & _max_procs = 0
       --> @dm:map-multi-proc attr = 'host_os_process_ppid' & func = 'strip' & _max_procs = 0
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Process'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_os_process_name,host_os_process_pid,host_os_process_ppid' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-windows_os_processes_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows OS Services
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_services_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> @dm:fixcolumns
       --> *dm:safe-filter * get name as 'host_os_service_name',display_name as 'host_os_service_description',process_id as 'host_os_service_pid',state as 'host_os_service_state', start_mode as 'host_os_service_mode', status as 'host_os_service_status',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:to-type columns = 'host_os_service_pid' & type = 'int'
       --> @dm:map-multi-proc attr = 'host_os_service_pid' & func = 'strip' & _max_procs = 0
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:eval asset_object = "'Service'"
       --> @dm:map to = 'asset_status' & func = 'fixed' & value = 'Active'
       --> @dm:map from = 'hostname,host_os_ip,host_os_service_name,host_os_service_description' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-windows_os_services_processed'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Process Windows Netstat TCP/UDP Connections
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_netstat_inventory' & return_empty = 'yes'
    --> *exec:if-condition collection_status = 'Success'
       --> *dm:safe-filter state does not contain '_' and pname is not empty
       --> @dm:dedup columns = 'localAddr,pid,pname,protocol,foreignAddr,foreignPort,state'
       --> *dm:safe-filter * get localAddr as 'local_ip_address',localPort as 'local_port',foreignAddr as 'remote_ip_address',foreignPort as 'remote_port',protocol as 'connection_protocol',pname as 'host_os_process_name',pid as 'host_os_process_pid',state as 'connection_state',collection_status,reason,collection_timestamp,collection_duration,source_ip as 'host_os_ip'
       --> @dm:map-multi-proc attr = 'local_ip_address' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'local_port' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'remote_ip_address' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'remote_port' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'host_os_process_name' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'host_os_process_pid' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'connection_protocol' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'connection_state' & func = 'strip' & _max_procs = 4
       --> @dm:map-multi-proc attr = 'collection_timestamp' & func = 'ts_to_datetimestr' & unit = 'ms' & _max_procs = 0
       --> @dm:enrich dict='temp-windows_host_system_info' & src_key_cols = 'host_os_ip' & dict_key_cols = 'host_os_ip' & enrich_cols = 'host_os_vendor,hostname,host_machine_type,host_os_version,host_machine_vendor,host_bios_uuid'
       --> @dm:to-type columns = 'local_port,remote_port,host_os_process_pid' & type = 'int'
       --> @dm:save name = 'temp-windows_os_netstat_processed'
    --> @exec:end-if

## ### Application to Application Dependency ###########
## Step-01 - Establish Server Port
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_netstat_processed' & empty_as_null = 'no' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:fixnull columns = 'connection_state,local_ip_address,remote_ip_address,remote_port'
       --> @dm:copy-columns from = 'local_ip_address' & to = 'local_ip_address_orig'
       --> @dm:map-multi-proc to = "local_ip_address" & func = "evaluate" & expr = "host_os_ip if local_ip_address == '127.0.0.1' or local_ip_address == '0.0.0.0' or local_ip_address == '::' or local_ip_address == '::1' or local_ip_address != host_os_ip else local_ip_address" & _max_procs = 0
       --> *dm:filter local_ip_address does not contain '::'
       --> *dm:filter connection_state contain "LISTEN" or (connection_state == "" and (remote_port == "*" or remote_port == ""))
       --> @dm:dedup columns = 'host_os_ip,local_port,connection_protocol'
       --> @dm:map-multi-proc to = "server_port_check" & func = "evaluate" & expr = "'Yes' if local_port != '0' else 'No'" & _max_procs = 0
       --> @dm:selectcolumns exclude = "^remote_.*$"
       --> @dm:eval asset_object = "'NetstatServerPorts'"
       --> @dm:eval asset_status = "'Active'"
       --> @dm:to-type columns = 'local_port,host_os_process_pid' & type = 'int'
       --> @dm:map from = 'host_os_ip,local_ip_address,local_port,host_os_process_name,host_os_process_pid,connection_protocol' & to = 'unique_id' & func = 'join' & sep = '_'
       --> @dm:dedup columns = 'unique_id'
       --> @dm:save name = 'temp-windows_netstat_server_port_dict'
       --> @rn:write-stream name = 'host-os-inventory'
    --> @exec:end-if

## Step-02 - Establish Client - Server Relationship
--> @c:new-block
    --> @dm:recall name = "temp-windows_os_netstat_processed" & empty_as_null = 'no' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:fixnull columns = 'connection_state,local_ip_address,remote_ip_address,remote_port'
       --> *dm:filter connection_state does not contain "LISTEN" and (connection_state != "" or remote_port != "*") and local_port != remote_port and local_ip_address != '127.0.0.1' and remote_ip_address != '127.0.0.1' and remote_port != ""
       --> *exec:if-shape num_rows > 0
          --> @dm:copy-columns from = 'local_ip_address' & to = 'local_ip_address_orig'
          --> @dm:copy-columns from = 'remote_ip_address' & to = 'remote_ip_address_orig'
          --> @dm:map-multi-proc to = "local_ip_address" & func = "evaluate" & expr = "host_os_ip if local_ip_address == '127.0.0.1' or local_ip_address == '0.0.0.0' or local_ip_address != host_os_ip or '::' in local_ip_address else local_ip_address" & _max_procs = 0
          --> @dm:map-multi-proc to = "remote_ip_address" & func = "evaluate" & expr = "host_os_ip if remote_ip_address == '127.0.0.1' or remote_ip_address == '0.0.0.0' or local_ip_address == remote_ip_address or '::' in remote_ip_address else remote_ip_address" & _max_procs = 0
          --> @dm:enrich dict = 'temp-windows_netstat_server_port_dict' & src_key_cols = 'local_ip_address,local_port,connection_protocol' & dict_key_cols = 'local_ip_address,local_port,connection_protocol' & enrich_cols = 'server_port_check'
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
          --> @dm:to-type columns = 'client_port,server_port,host_os_process_pid' & type = 'int'
          --> @dm:map from = 'client_ip_address,client_port,server_ip_address,server_port,connection_state,connection_protocol' & to = 'unique_id' & func = 'join' & sep = '_'
          --> @dm:dedup columns = 'unique_id'
          --> @dm:save name = 'temp-windows_netstat_client_server_dict'
          --> @rn:write-stream name = 'host-os-inventory'
       --> @exec:end-if
    --> @exec:end-if

## Host OS Topology Nodes + VMware Virtual Machine Node ID Lookup Enrichment
--> @c:new-block
    --> @dm:recall name = 'temp-vcenter-vm-nodes-dict' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:recall name = 'temp-windows_host_system_info' & return_empty = 'yes'
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
       --> @dm:save name = 'temp-windows_system_info_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if
    --> *exec:if-shape num_rows = 0
       --> @dm:recall name = 'temp-windows_host_system_info' & return_empty = 'yes'
       --> *dm:safe-filter collection_status = 'Success'
       --> @dm:eval layer = "'Compute'"
       --> @dm:eval node_type = "'Host_OS'"
       --> @dm:eval iconURL = "'Host'"
       --> @dm:rename-columns node_id = 'unique_id'
       --> @dm:map from = 'hostname' & to = 'node_label'
       --> @dm:save name = 'temp-windows_system_info_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## Host OS Services Topology Nodes
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_services_processed' & return_empty = 'yes'
    --> *exec:if-shape num_rows > 0
       --> @dm:map attr = 'host_os_service_state' & func = 'strip'
       --> *dm:filter collection_status = 'Success' and host_os_service_state = 'Running'
       --> @dm:eval layer = "'Application Component'"
       --> @dm:eval node_type = "'OS_Service'"
       --> @dm:eval iconURL = "'Service'"
       --> @dm:rename-columns node_id = 'unique_id'
       --> @dm:map from = 'hostname,host_os_service_name' & to = 'node_label' & func = 'join' & sep = '_'
       --> @dm:map from = 'hostname,host_os_ip,host_bios_uuid' & to = 'host_os_node_id' & func = 'join' & sep = '_'
       --> @dm:save name = 'temp-windows_os_services_nodes'
       --> @rn:write-stream name = 'cfx_rdaf_topology_nodes'
       --> @graph:insert-nodes dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_nodes' & key_column = 'node_id'
    --> @exec:end-if

## Host OS Topology Edges
## Host OS Nodes to VMware VM Relationship
--> @c:new-block
    --> @dm:recall name = 'temp-windows_system_info_nodes'
    --> *dm:safe-filter vm_node_id is not empty
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter * get vm_node_id as 'left_id',vm_node_label as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type'
       --> @dm:eval left_node_type = "'VM'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'runs'
       --> @dm:save name = 'temp-host-os-to-vm-edges'
    --> @exec:end-if

## Host OS Nodes to Services Relationship
--> @c:new-block
    --> @dm:recall name = 'temp-windows_os_services_nodes'
    --> *dm:safe-filter host_os_node_id is not empty
    --> *exec:if-shape num_rows > 0
       --> *dm:safe-filter * get host_os_node_id as 'left_id',hostname as 'left_label',node_id as 'right_id',node_label as 'right_label',node_type as 'right_node_type'
       --> @dm:eval left_node_type = "'Host_OS'"
       --> @dm:map to = 'relation_type' & func = 'fixed' & value = 'runs'
       --> @dm:save name = 'temp-host-os-to-services-edges'
    --> @exec:end-if

## Generate Host OS Edges Ingestion ID
--> @c:new-block
    --> @dm:empty
    --> @dm:addrow inventory_source = 'windows_os_system_inventory'
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
    --> @dm:eval inventory_source = "'windows_os_system_inventory'"
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> *dm:filter left_id != right_id
    --> *dm:filter left_label is not empty
    --> *dm:filter right_label is not empty
    --> @dm:dedup columns = 'left_id,right_id'
    --> @dm:enrich dict = 'temp-topology-system-edges-ingestion-id-dict' & src_key_cols = 'inventory_source' & dict_key_cols = 'inventory_source' & enrich_cols = 'topology_ingestion_id'
    --> *dm:filter * get left_label,left_id,left_node_type,relation_type,right_id,right_label,right_node_type,inventory_source,topology_ingestion_id,collection_timestamp
    --> @dm:save name = 'temp-host-os-nodes-topology-edges'
    --> @rn:write-stream name = 'cfx_rdaf_topology_edges'
    --> @graph:insert-edges dbname = 'cfx_rdaf_topology' & nodes_collection = 'cfx_rdaf_topology_nodes' & edges_collection = 'cfx_rdaf_topology_edges' & left_id = 'left_id' & right_id = 'right_id'

## Generate Host OS Services Edges Ingestion ID
--> @c:new-block
    --> @dm:empty
    --> @dm:addrow inventory_source = 'windows_os_services_inventory'
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> @dm:map from = 'collection_timestamp' & to = 'topology_ingestion_id' & func = 'md5'
    --> @dm:save name = 'temp-topology-service-edges-ingestion-id-dict'

## Host OS Service Node Topology - Edges
--> @c:new-block
    --> @dm:recall name = 'temp-host-os-to-services-edges' & return_empty = 'yes'
    --> @dm:skip-block-if-shape row_count = 0
    --> @dm:map attr = 'left_id' & func = 'strip'
    --> @dm:map attr = 'right_id' & func = 'strip'
    --> @dm:map from = 'left_id' & to = 'left'
    --> @dm:map from = 'right_id' & to = 'right'
    --> @dm:eval inventory_source = "'windows_os_services_inventory'"
    --> @dm:map to = 'collection_timestamp' & func = 'time_now'
    --> *dm:filter left_id != right_id
    --> *dm:filter left_label is not empty
    --> *dm:filter right_label is not empty
    --> @dm:dedup columns = 'left_id,right_id'
    --> @dm:enrich dict = 'temp-topology-service-edges-ingestion-id-dict' & src_key_cols = 'inventory_source' & dict_key_cols = 'inventory_source' & enrich_cols = 'topology_ingestion_id'
    --> *dm:filter * get left_label,left_id,left_node_type,relation_type,right_id,right_label,right_node_type,inventory_source,topology_ingestion_id,collection_timestamp
    --> @dm:save name = 'temp-host-os-services-nodes-topology-edges'
    --> @rn:write-stream name = 'cfx_rdaf_topology_edges'
    --> @graph:insert-edges dbname = 'cfx_rdaf_topology' & nodes_collection = 'cfx_rdaf_topology_nodes' & edges_collection = 'cfx_rdaf_topology_edges' & left_id = 'left_id' & right_id = 'right_id'

## Delete Stale Windows OS Topology Edges
--> @c:new-block
    --> @dm:empty
    --> @dm:sleep seconds = 60

## Delete Stale Windows OS System Topology Edges from Pstream
--> @c:new-block
    --> @dm:recall name = 'temp-topology-system-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> #dm:pstream-delete-data-by-query topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}' with-input name = 'cfx_rdaf_topology_edges' & timeout = 300
          --> @dm:save name = 'temp-windows_system_deleted_edges'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale Windows OS System Topology Edges from GraphDB
--> @c:new-block
    --> @dm:recall name = 'temp-topology-system-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> @graph:delete-by-query dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_edges' & delete_query = "topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}'"
          --> @dm:save name = 'temp-windows_system_deleted_edges_from_graphdb'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale Windows OS Service Topology Edges from Pstream
--> @c:new-block
    --> @dm:recall name = 'temp-topology-service-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> #dm:pstream-delete-data-by-query topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}' with-input name = 'cfx_rdaf_topology_edges' & timeout = 300
          --> @dm:save name = 'temp-windows_service_deleted_edges'
       --> @exec:end-loop
    --> @exec:end-if

## Delete Stale Windows OS Service Topology Edges from GraphDB
--> @c:new-block
    --> @dm:recall name = 'temp-topology-service-edges-ingestion-id-dict' & return_empty = 'yes'
    --> *dm:safe-filter topology_ingestion_id is not empty & inventory_source is not empty
    --> *exec:if-shape num_rows > 0
       --> @dm:dedup columns = 'topology_ingestion_id,inventory_source'
       --> @exec:for-loop num_rows = 1
          --> @graph:delete-by-query dbname = 'cfx_rdaf_topology' & collection = 'cfx_rdaf_topology_edges' & delete_query = "topology_ingestion_id != '{{row.topology_ingestion_id}}' & inventory_source = '{{row.inventory_source}}'"
          --> @dm:save name = 'temp-windows_service_deleted_edges_from_graphdb'
       --> @exec:end-loop
    --> @exec:end-if

