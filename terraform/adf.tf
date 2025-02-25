resource "azurerm_data_factory" "datafactory" {
  name                = var.data_factory_name // Data Factory name from variable
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  // Additional configuration or tagging can be added as needed
}

resource "azurerm_data_factory_linked_service_azure_blob_storage" "taxi_data" {
  name              = "taxi_data"
  data_factory_id   = azurerm_data_factory.datafactory.id
  connection_string = azurerm_storage_account.source_taxi_data_storage.primary_connection_string
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "bronze_layer" {
  name            = "bronze_layer"
  data_factory_id = azurerm_data_factory.datafactory.id
  url             = "https://${azurerm_storage_account.storage.name}.dfs.core.windows.net"

  # Build the connection string using the storage account's name and key
  storage_account_key = azurerm_storage_account.storage.primary_access_key
}

resource "azurerm_data_factory_dataset_delimited_text" "source_taxi_rides_csv" {
  name                = "source_taxi_rides_csv"
  data_factory_id     = azurerm_data_factory.datafactory.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.taxi_data.name

  azure_blob_storage_location {
    container = azurerm_storage_container.taxi_rides_hist.name
  }

  column_delimiter    = ","
  row_delimiter       = "\n"
  escape_character    = "\\"
  first_row_as_header = true
  quote_character     = "\""
}

resource "azurerm_data_factory_dataset_delimited_text" "source_dim_tables_csv" {
  name                = "source_dim_tables_csv"
  data_factory_id     = azurerm_data_factory.datafactory.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.taxi_data.name

  azure_blob_storage_location {
    container = azurerm_storage_container.taxi_dim_tables.name
  }

  column_delimiter    = ","
  row_delimiter       = "\n"
  escape_character    = "\\"
  first_row_as_header = true
  quote_character     = "\""
}

resource "azurerm_data_factory_dataset_parquet" "bronze_taxi_rides" {
  name                = "bronze_taxi_rides"
  data_factory_id     = azurerm_data_factory.datafactory.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.bronze_layer.name

  azure_blob_storage_location {
    container = "bronze"
    path      = "fact_taxi_rides_hist"
  }

  compression_codec = "snappy"
}

resource "azurerm_data_factory_dataset_parquet" "bronze_dim_tables" {
  name                = "bronze_dim_tables"
  data_factory_id     = azurerm_data_factory.datafactory.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.bronze_layer.name

  parameters = {
    folderName = "String"
  }

  azure_blob_storage_location {
    container = "bronze"
    path      = "@{dataset().folderName}"
  }

  compression_codec = "snappy"
}




resource "azurerm_data_factory_pipeline" "ingestion_pipeline" {
  name            = "ingestion_pipeline"
  data_factory_id = azurerm_data_factory.datafactory.id
  activities_json = <<JSON
[
            {
                "name": "ingest_taxi_rides",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "ForEach_DimTables",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "AzureBlobStorageReadSettings",
                            "recursive": true,
                            "wildcardFileName": "*",
                            "enablePartitionDiscovery": false
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings"
                        }
                    },
                    "sink": {
                        "type": "ParquetSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings",
                            "copyBehavior": "FlattenHierarchy"
                        },
                        "formatSettings": {
                            "type": "ParquetWriteSettings"
                        },
                        "additionalColumns": {
                            "ingestion_time": {
                            "value": "@utcnow()"
                            }
                        }
                    },
                    "enableStaging": false,
                    "translator": {
                        "type": "TabularTranslator",
                        "typeConversion": true,
                        "typeConversionSettings": {
                            "allowDataTruncation": true,
                            "treatBooleanAsNumber": false
                        }
                    }
                },
                "inputs": [
                    {
                        "referenceName": "${azurerm_data_factory_dataset_delimited_text.source_taxi_rides_csv.name}",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "${azurerm_data_factory_dataset_parquet.bronze_taxi_rides.name}",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "GetMetadata_DimTables",
                "type": "GetMetadata",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "dataset": {
                        "referenceName": "${azurerm_data_factory_dataset_delimited_text.source_dim_tables_csv.name}",
                        "type": "DatasetReference"
                    },
                    "fieldList": [
                        "childItems"
                    ],
                    "storeSettings": {
                        "type": "AzureBlobStorageReadSettings",
                        "recursive": true,
                        "enablePartitionDiscovery": false
                    },
                    "formatSettings": {
                        "type": "DelimitedTextReadSettings"
                    }
                }
            },
            {
                "name": "ForEach_DimTables",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "GetMetadata_DimTables",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@activity('GetMetadata_DimTables').output.childItems",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "Copy_DimTable",
                            "type": "Copy",
                            "dependsOn": [],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "source": {
                                    "type": "DelimitedTextSource",
                                    "storeSettings": {
                                        "type": "AzureBlobStorageReadSettings",
                                        "recursive": true,
                                        "wildcardFileName": {
                                            "value": "@item().name",
                                            "type": "Expression"
                                        },
                                        "enablePartitionDiscovery": false
                                    },
                                    "formatSettings": {
                                        "type": "DelimitedTextReadSettings"
                                    }
                                },
                                "sink": {
                                    "type": "ParquetSink",
                                    "storeSettings": {
                                        "type": "AzureBlobFSWriteSettings",
                                        "copyBehavior": "FlattenHierarchy"
                                    },
                                    "formatSettings": {
                                        "type": "ParquetWriteSettings"
                                    },
                                    "additionalColumns": {
                                        "ingestion_time": {
                                        "value": "@utcnow()"
                                        }
                                    }
                                },
                                "enableStaging": false,
                                "translator": {
                                    "type": "TabularTranslator",
                                    "typeConversion": true,
                                    "typeConversionSettings": {
                                        "allowDataTruncation": true,
                                        "treatBooleanAsNumber": false
                                    }
                                }
                            },
                            "inputs": [
                                {
                                    "referenceName": "${azurerm_data_factory_dataset_delimited_text.source_dim_tables_csv.name}",
                                    "type": "DatasetReference"
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": "${azurerm_data_factory_dataset_parquet.bronze_dim_tables.name}",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "folderName": "@concat(split(item().name, '.')[0],'/')"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        ]
  JSON
}