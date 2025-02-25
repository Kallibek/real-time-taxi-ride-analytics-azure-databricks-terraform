resource "azurerm_databricks_workspace" "databricks_ws" {
  name                = "databricks-ws"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
}


provider "databricks" {
  host                        = azurerm_databricks_workspace.databricks_ws.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.databricks_ws.id
  azure_client_id             = var.client_id
  azure_client_secret         = var.client_secret
  azure_tenant_id             = var.tenant_id
}

resource "databricks_secret_scope" "terraform" {
  name                     = "application"
  initial_manage_principal = "users"
}

resource "databricks_secret" "storage_key" {
  key          = "blob_storage_key"
  string_value = azurerm_storage_account.storage.primary_access_key
  scope        = databricks_secret_scope.terraform.name
}

locals {
  containers = ["bronze", "silver", "gold"]
}

resource "databricks_cluster" "my_cluster" {
  cluster_name            = "my-cluster"
  spark_version           = "15.4.x-scala2.12"
  node_type_id            = "Standard_D4plds_v6"
  autotermination_minutes = 20
  num_workers             = 1
}


resource "databricks_mount" "mounts" {
  for_each   = toset(local.containers)
  name       = each.key
  cluster_id = databricks_cluster.my_cluster.cluster_id
  wasb {
    container_name       = each.key
    storage_account_name = azurerm_storage_account.storage.name
    auth_type            = "ACCESS_KEY"
    token_secret_scope   = databricks_secret_scope.terraform.name
    token_secret_key     = databricks_secret.storage_key.key
  }
}


resource "databricks_secret" "eventhub_conn_str" {
  key          = "eventhub_conn_str"
  string_value = azurerm_eventhub_authorization_rule.taxi_auth.primary_connection_string
  scope        = databricks_secret_scope.terraform.name
}

//////////////////////////////////////////////////////////
// Upload Local Notebooks to Databricks Workspace
//////////////////////////////////////////////////////////

resource "databricks_notebook" "notebook_bronze_to_silver_batch_dim_tables" {
  path   = "/notebooks/bronze_to_silver_batch_dim_tables"
  source = "../notebooks/bronze_to_silver_batch_dim_tables.py"
}

resource "databricks_notebook" "notebook_bronze_to_silver_batch_taxi_rides_historical" {
  path   = "/notebooks/bronze_to_silver_batch_taxi_rides_historical"
  source = "../notebooks/bronze_to_silver_batch_taxi_rides_historical.py"
}

resource "databricks_notebook" "notebook_source_to_bronze_streaming" {
  path   = "/notebooks/source_to_bronze_streaming"
  source = "../notebooks/source_to_bronze_streaming.py"
}

resource "databricks_notebook" "notebook_bronze_to_silver_streaming" {
  path   = "/notebooks/bronze_to_silver_streaming"
  source = "../notebooks/bronze_to_silver_streaming.py"
}

resource "databricks_notebook" "notebook_silver_to_gold" {
  path   = "/notebooks/silver_to_gold"
  source = "../notebooks/silver_to_gold.py"
}

resource "databricks_notebook" "notebook_dashboard" {
  path   = "/notebooks/dashboard"
  source = "../notebooks/dashboard.py"
}


resource "databricks_job" "batch_workflow" {
  name = "databricks_batch_workflow"

  schedule {
    # Runs once a day at midnight UTC; adjust the cron expression and timezone as needed.
    quartz_cron_expression = "0 0 0 * * ?"
    timezone_id            = "UTC"
  }

  # Task 1: Batch Notebook – Dim Tables
  task {
    task_key = "batch_dim_tables"
    notebook_task {
      notebook_path = "/notebooks/bronze_to_silver_batch_dim_tables"
    }
    existing_cluster_id = databricks_cluster.my_cluster.cluster_id
  }

  # Task 2: Batch Notebook – Taxi Rides Historical
  task {
    task_key = "batch_taxi_rides_historical"
    depends_on {
      task_key = "batch_dim_tables"
    }
    notebook_task {
      notebook_path = "/notebooks/bronze_to_silver_batch_taxi_rides_historical"
    }
    existing_cluster_id = databricks_cluster.my_cluster.cluster_id
  }
}

resource "databricks_job" "streaming_workflow" {
  name = "databricks_streaming_workflow"

  # Task 1: Streaming Notebook – Source to Bronze
  task {
    task_key = "source_to_bronze_streaming"
    notebook_task {
      notebook_path = "/notebooks/source_to_bronze_streaming"
    }
    existing_cluster_id = databricks_cluster.my_cluster.cluster_id
    timeout_seconds     = 0
  }

  # Task 2: Streaming Notebook – Bronze to Silver
  task {
    task_key = "bronze_to_silver_streaming"
    notebook_task {
      notebook_path = "/notebooks/bronze_to_silver_streaming"
    }
    existing_cluster_id = databricks_cluster.my_cluster.cluster_id
    timeout_seconds     = 0
  }

  # Task 3: Notebook – Silver to Gold
  task {
    task_key = "silver_to_gold"
    notebook_task {
      notebook_path = "/notebooks/silver_to_gold"
    }
    existing_cluster_id = databricks_cluster.my_cluster.cluster_id
  }
}
