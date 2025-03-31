**Topology Configuration:**

To visualize the topology data, **Stacks** and **Relationship Maps** need to be configured.

  * Create a Stack Definition
  * Create a Relationship map

**Stack Definition:**

 Login into CloudFabrix RDAF Platform as MSP Admin User, go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Stacks" --> Click on **Add Stack**

 * Enter **Stack Name** as **ADM_Topology_using_Graph**

 * Enter **Description** as **IT infrastructure and Application Dependency Mappings Using GraphDB**

 * **Stack Type** as **Dynamic**

 * Under **Stack Data**, enter the below JSON configuration and click on **Save** button

    ```
      {
          "name": "ADM_Topology_using_Graph",
          "description": "IT infrastructure and Application Dependency Mappings Using GraphDB",
          "is_dynamic": true,
          "hierarchical": true,
          "dynamic_nodes": {
              "column_name": "node_type",
              "db_name": "cfx_rdaf_topology",
              "nodes_collection": "cfx_rdaf_topology_nodes",
              "stream": "graphdb://cfx_rdaf_topology/cfx_rdaf_topology_nodes"
          },
          "dynamic_relationships": {
              "db_name": "cfx_rdaf_topology",
              "edges_collection": "cfx_rdaf_topology_edges",
              "graph_name": "cfx_rdaf_topology_graph",
              "relation_map": "rdaf_topology_relationships",
              "stream": "graphdb://cfx_rdaf_topology/cfx_rdaf_topology_edges"
          }
      }

    ```

**Relationship Maps:**

  Login into CloudFabrix RDAF Platform as MSP Admin User, go to "Main Menu" --> "Configuration" --> "RDA Administration" --> "Stacks" --> Click on **Relationship Maps** --> Click on **Add**

  * Copy the contents of **relationship_maps.json** file and click on **Save** button

