**Pipeline Name:** linux_inventory_and_topology_using_winrm_v1.apl

**About this Pipeline:**

It collects Linux OS inventory executing CLI commands over SSH protocol. For detailed information on each inventory data collection, please refer: https://bot-docs.cloudfabrix.io/Datasource_Integrations/Linux-OS/

**Pre-requisites:**

* For network port access and user permissions, please refer: https://bot-docs.cloudfabrix.io/Datasource_Integrations/Linux-OS/

* **CloudFabrix RDAF Platform:**

  * RDAF Platform running with 3.5 or above version installed with GrapDB database
  * Add Linux OS Credentials
  * Create a Pstream for Linux Inventory
  * Create a Pstream for Topology Nodes and Edges
  * Create a GraphDB database and collection for Nodes and Edges
  * Create or Update the Linux host ip list dataset (i.e. asset_discovery_master_list)

* **Add Linux OS Credentials:**

    Login into CloudFabrix RDAF Platform as MSP Admin User, go to "Main Menu" --> "Configuration" --> "RDA Integration" --> "Credentials" --> Click on "Add"

    Select Secret Type as "linux-inventory"

    Enter Name for Windows Credential name as "linux"

    Enter Linux IP (this is for a quick credentials validation only)

    Enter Username & Password or Enter SSH Key details

    Leave the port 22 as default, change it if the target Linux hosts are running on custom SSH port.

    Select Worker site and Click on "Check Connectivity", if the RDA Worker is able to reach the Linux host and authentication goes through, the status will show as "OK"

    Click on "Save" to save the Linux OS credentials.

* **Create Pstreams for Linux Inventory:** (Note: If it is already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Persistent Streams" --> "Persistent Streams" --> Click on "Add"

    **Create below Pstreams:**

    Pstream Name: host-os-inventory

    Attribute Settings:

      {
        "unique_keys": [
            "unique_id"
        ],
        "search_case_insensitive": true,
        "_settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "30s"
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

* **Create or Update the Linux host ip list dataset:**

    The inventory pipeline expects the below dataset to exist with specific columns.

    Dataset Name: **asset_discovery_master_list** (csv format)
    Columns: ip_address,type,port,discovery_scope
    
    * ip_address (Mandatory)
    * type (Mandatory: ex value: linux)
    * port (Optional)
    * discovery_scope (Mandatory: values: yes or no)

    Sample Data: 192.168.10.10,linux,22,yes

    Create a CSV file with above columns with Linux host IP list and import it as a dataset in CloudFabrix RDAF Platform.

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Datasets" --> "Datasets" --> Click on "Add Dataset"

    Enter the Dataset name as "asset_discovery_master_list", choose CSV file to upload that has above mentioned columns and Click on "Add" button to save the dataset.



