# Architecture Diagram

```mermaid
flowchart TD
    subgraph Source["Source"]
        KAGGLE[Kaggle Dataset<br/>transactions, users, cards,<br/>mcc_codes, fraud_labels]
    end

    subgraph Storage["ADLS Gen2 - storagefinancialfraud"]
        LANDING[("landing/<br/>partitioned by ingestion_date")]
        BRONZE[("bronze/<br/>External Delta, all-String")]
        SILVER[("silver/<br/>External Delta, star schema")]
        GOLD[("gold/<br/>External Delta, aggregated marts")]
    end

    subgraph ADF["Azure Data Factory"]
        PL_MASTER["pl_master_orchestration"]
        PL_BRONZE["pl_landing_to_bronze<br/>(Lookup + ForEach)"]
        PL_SILVER["pl_bronze_to_silver<br/>(Lookup + ForEach)"]
        PL_GOLD["pl_silver_to_gold<br/>(Lookup + ForEach)"]
    end

    subgraph Databricks["Azure Databricks (Premium, Unity Catalog)"]
        NB_BRONZE["01_bronze_ingestion.py"]
        NB_SILVER["02_silver_transformation.py"]
        NB_GOLD["03_gold_aggregation.py"]
        UTILS["utils/ (transformation_utils, logging_utils)"]
        LOGS[("control.pipeline_logs")]
    end

    subgraph Serving["Serving Layer"]
        SYNAPSE["Synapse Serverless SQL<br/>finance_fraud_serving views"]
        PBI["Power BI<br/>(Import mode dashboard)"]
    end

    subgraph Security["Security & Config"]
        KV["Key Vault<br/>(RBAC, secrets)"]
        CONFIG["_config/*.json<br/>entity / silver / gold table config"]
    end

    KAGGLE -->|manual upload| LANDING
    CONFIG -.->|drives Lookup activities| ADF

    PL_MASTER --> PL_BRONZE --> PL_SILVER --> PL_GOLD

    PL_BRONZE -->|triggers| NB_BRONZE
    PL_SILVER -->|triggers| NB_SILVER
    PL_GOLD -->|triggers| NB_GOLD

    LANDING --> NB_BRONZE --> BRONZE
    BRONZE --> NB_SILVER --> SILVER
    SILVER --> NB_GOLD --> GOLD

    NB_BRONZE -.-> UTILS
    NB_SILVER -.-> UTILS
    NB_BRONZE -.->|logs| LOGS
    NB_SILVER -.->|logs| LOGS

    KV -.->|secrets via managed identity| ADF
    KV -.->|secret scope| Databricks

    GOLD --> SYNAPSE --> PBI
```

## Notes on the diagram

- Dotted lines represent configuration/security relationships (secrets,
  logging, config-driven behavior) rather than data flow itself.
- The master orchestrator (`pl_master_orchestration`) calls the three layer
  pipelines in sequence via `Execute Pipeline` activities; each layer
  pipeline is also independently runnable/debuggable on its own.
- Not shown for clarity: the Databricks Access Connector + Unity Catalog
  Storage Credential + External Location chain that authorizes table
  registration, and the AzureDatabricks/ADF/Synapse managed identity role
  assignments on Key Vault and storage — both covered in
  `docs/design_decisions.md`.