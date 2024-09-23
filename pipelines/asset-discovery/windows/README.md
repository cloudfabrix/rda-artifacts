Pipeline Name: windows_inventory_and_topology_using_winrm_v1.apl

About this Pipeline:

It collects Windows OS inventory executing Windows CLI commands over WinRM protocol. For detailed information on each inventory data collection, please refer: https://bot-docs.cloudfabrix.io/Datasource_Integrations/Microsoft-Windows-Server-OS/

Pre-requisites:

* For network port access and user permissions, please refer: https://bot-docs.cloudfabrix.io/Datasource_Integrations/Microsoft-Windows-Server-OS/

* CloudFabrix RDAF Platform:

  * RDAF Platform running with 3.5 or above version installed with GrapDB database
  * Add Windows OS Credentials
  * Create a Pstream for Windows Inventory
  * Create a Pstream for Topology Nodes and Edges
  * Create a GraphDB database and collection for Nodes and Edges

* Add Windows OS Credentials:

    Login into CloudFabrix RDAF Platform as MSP Admin User, go to "Main Menu" --> "Configuration" --> "RDA Integration" --> "Credentials" --> Click on "Add"

    Select Secret Type as "windows-inventory"

    Enter Name for Windows Credential name.

    Enter Windows IP (this is for a quick credentials validation only)

    Enter Username

    Enter Password

    Leave the rest as defaults (Note: Change these only if WinRM is configured with SSL configuration)

    Select Worker site and Click on "Check Connectivity", if the RDA Worker is able to reach the Windows host and authentication goes through, the status will show as "OK"

    Click on "Save" to save the Windwos credentials.

* Create Pstreams for Windows Inventory: (Note: If it is already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Persistent Streams" --> "Persistent Streams" --> Click on "Add"

    Create below Pstreams:

    Pstream Name: host-os-system-inventory
    Pstream Name: host-os-service-inventory
    Pstream Name: host-os-netstat-inventory

    Attribute Settings: (same for all of the above)

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

* Create Pstreams for Topology Nodes and Edges: (Note: If it is already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Persistent Streams" --> "Persistent Streams" --> Click on "Add"

    Create below Pstreams:

    Pstream Name: cfx_rdaf_topology_nodes

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

    Pstream Name: cfx_rdaf_topology_edges

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

* **Create GraphDB for Topology Nodes and Edges:** (Note: If it is already exist, please ignore)

    Go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Graph DB" --> "Graphs" --> Click on "Add"

    Enter Database Name as "cfx_rdaf_topology" and Click on "Save". It will create the below.

    Graph Name as "cfx_rdaf_topology_graph"
    Graph Database as "cfx_rdaf_topology"
    Graph Collection for Topology Nodes as "cfx_rdaf_topology_nodes"
    Graph Collection for Topology Edges as "cfx_rdaf_topology_edges"

