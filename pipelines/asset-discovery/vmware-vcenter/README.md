**Pipelines:**

  * vmware_vcenter_source_name_inventory_v1.apl
  * vmware_vcenter_inventory_pipeline_v1.apl
  * vmware_vcenter_source_name_topology_v1.apl
  * vmware_vcenter_topology_v1.apl

**About VMware vCenter Pipelines:**

These collect VMware vCenter inventory such as Datacenters, Clusters, ESXi Hosts, VirtualMachines, Datastores, vSwitches, dvSwitches etc. For detailed information on how to integrate with VMware vCenter, please refer: https://bot-docs.cloudfabrix.io/Datasource_Integrations/VMware-vCenter/

**Pre-requisites:**

* For network port access and user permissions, please refer: https://bot-docs.cloudfabrix.io/Datasource_Integrations/VMware-vCenter/

* **CloudFabrix RDAF Platform:**

  * RDAF Platform running with 3.5 or above version installed with GrapDB database
  * Add VMware vCenter User Credentials
  * Create a Pstream for VMware vCenter
  * Create a Pstream for Topology Nodes and Edges
  * Create a GraphDB database and collection for Nodes and Edges

* **Add VMware vCenter Credentials:**

    Login into CloudFabrix RDAF Platform as MSP Admin User, go to "Main Menu" --> "Configuration" --> "RDA Integration" --> "Credentials" --> Click on "Add"

    Select Secret Type as "vmware-vcenter-v2" (**Note**: Please note "vmware-vcenter" secret type is deprecated and not supported)

    Enter Name for VMware vCenter (e.g, vcenter_prod)

    Enter vCenter IP or DNS Name under **Hostname** field

    Enter Username (e.g, readonly@vsphere.local)

    Enter Password

    Leave the Timeout 30 (seconds) as default, or increase it to 60 or more if needed.

    Select the worker site and click on **Check Connectivity** If the RDA Worker can reach the VMware vCenter host and authentication is successful, the status will display as **OK**

    Click on **Save** to save the VMware vCenter credentials.

* **Create Pstreams for VMware vCenter Inventory:** (Note: If it is already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Persistent Streams" --> "Persistent Streams" --> Click on "Add"

    **Create below Pstreams:**

    Pstream Name: vmware-vcenter-inventory

    Attribute Settings:

      {
            "unique_keys": [
                "unique_id"
            ],
            "timestamp": "collection_timestamp",
            "retention_days": 5,
            "retention_purge_extra_filter": "asset_status = 'Purge'",
            "search_case_insensitive": true,
            "_settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "refresh_interval": "30s"
            },
            "_mappings": {
                "properties": {
                    "cpu_capacity_ghz": {
                        "type": "double"
                    },
                    "cpu_used_ghz": {
                        "type": "double"
                    },
                    "cpu_free_ghz": {
                        "type": "double"
                    },
                    "memory_capacity_gb": {
                        "type": "double"
                    },
                    "memory_free_gb": {
                        "type": "double"
                    },
                    "storage_capacity_gb": {
                        "type": "double"
                    },
                    "storage_used_gb": {
                        "type": "double"
                    },
                    "storage_free_gb": {
                        "type": "double"
                    },
                    "num_of_datastores": {
                        "type": "double"
                    },
                    "num_of_hosts": {
                        "type": "double"
                    },
                    "num_of_networks": {
                        "type": "double"
                    },
                    "num_of_vms": {
                        "type": "double"
                    },
                    "vm_cpus": {
                        "type": "double"
                    },
                    "vm_cpu_sockets": {
                        "type": "double"
                    },
                    "vm_cores_per_socket": {
                        "type": "double"
                    },
                    "vm_cpu_shares": {
                        "type": "double"
                    },
                    "vm_cpu_reservation_mhz": {
                        "type": "double"
                    },
                    "vm_memory_mb": {
                        "type": "double"
                    },
                    "vm_memory_shares": {
                        "type": "double"
                    },
                    "vm_memory_reservation_mb": {
                        "type": "double"
                    },
                    "vm_nics": {
                        "type": "double"
                    },
                    "vm_disks": {
                        "type": "double"
                    },
                    "esxi_host_cpu_sockets": {
                        "type": "double"
                    },
                    "esxi_host_cpu_cores": {
                        "type": "double"
                    },
                    "esxi_host_cpu_threads": {
                        "type": "double"
                    },
                    "esxi_host_memory_bytes": {
                        "type": "double"
                    },
                    "esxi_host_hbas": {
                        "type": "double"
                    },
                    "esxi_host_nics": {
                        "type": "double"
                    },
                    "esxi_host_vms": {
                        "type": "double"
                    },
                    "esxi_host_vcpus_per_core": {
                        "type": "double"
                    },
                    "esxi_host_vcpus": {
                        "type": "double"
                    },
                    "esxi_host_provisioned_memory_gb": {
                        "type": "double"
                    },
                    "esxi_host_cpus": {
                        "type": "double"
                    },
                    "esxi_host_memory_gb": {
                        "type": "double"
                    },
                    "esxi_host_available_memory_gb": {
                        "type": "double"
                    },
                    "esxi_host_overprovision_memory_perc": {
                        "type": "double"
                    },
                    "datastore_total_gb": {
                        "type": "double"
                    },
                    "datastore_free_gb": {
                        "type": "double"
                    },
                    "datastore_provisioned_gb": {
                        "type": "double"
                    },
                    "datastore_overprovisioned_pct": {
                        "type": "double"
                    },
                    "datastore_hosts": {
                        "type": "double"
                    },
                    "datastore_used_gb": {
                        "type": "double"
                    },
                    "all_vms_capacity_gb": {
                        "type": "double"
                    },
                    "powered_on_vms_capacity_gb": {
                        "type": "double"
                    },
                    "powered_off_vms_capacity_gb": {
                        "type": "double"
                    }
                }
            }
        }

    Pstream Name: network-endpoints-identity-stream (Note: If it is already exist, please ignore)

    Attribute Settings:

      {
        "unique_keys": [
            "unique_id"
        ],
        "timestamp": "collection_timestamp",
        "retention_days": 3,
        "search_case_insensitive": true,
        "_settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "30s"
        }
      }
      

* **Create Pstreams for Topology Nodes and Edges:** (Note: If they are already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Persistent Streams" --> "Persistent Streams" --> Click on "Add"

    Create below Pstreams:

    Pstream Name: cfx_rdaf_topology_nodes (Note: If it is already exist, please ignore)

    Attribute Settings: 

      
      {
        "unique_keys": [
            "node_id"
        ],
        "default_values": {
            "iconURL": "Not Available"
        },
        "search_case_insensitive": true,
        "_settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "1s"
        }
      }
      

    Pstream Name: cfx_rdaf_topology_edges (Note: If it is already exist, please ignore)

    Attribute Settings: 

      
      {
        "unique_keys": [
            "left_id",
            "right_id"
        ],
        "search_case_insensitive": true,
        "_settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "1s"
        }
      }
      

* **Create GraphDB for Topology Nodes and Edges:** (Note: If they are already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Graph DB" --> "Graphs" --> Click on "Add" (Note: If it is already exist, please ignore)

    Enter Database Name as "cfx_rdaf_topology" and Click on "Save". It will create the below.

    Graph Name as "cfx_rdaf_topology_graph"

    Graph Database as "cfx_rdaf_topology"

    Graph Collection for Topology Nodes as "cfx_rdaf_topology_nodes"

    Graph Collection for Topology Edges as "cfx_rdaf_topology_edges"

