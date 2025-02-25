# Create an Event Hub namespace with a taxi-relevant name
resource "azurerm_eventhub_namespace" "taxi_ns" {
  name                = "taxi-data-analytics-ehns"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
}

# Create an Event Hub within the namespace for ingesting taxi event data
resource "azurerm_eventhub" "taxi_eventhub" {
  name                = "taxi-data-events"
  namespace_name      = azurerm_eventhub_namespace.taxi_ns.name
  resource_group_name = azurerm_resource_group.rg.name

  partition_count   = 1 # Adjust based on your scaling needs
  message_retention = 1 # Retention in days; modify as required
}

# Create an authorization rule to allow sending data to the Event Hub
resource "azurerm_eventhub_authorization_rule" "taxi_auth" {
  name                = "send-policy"
  eventhub_name       = azurerm_eventhub.taxi_eventhub.name
  namespace_name      = azurerm_eventhub_namespace.taxi_ns.name
  resource_group_name = azurerm_resource_group.rg.name

  listen = true
  send   = true
  manage = false
}

# Save the connection string to a file for your Python worker
resource "local_file" "taxi_conn_string_file" {
  filename = "../src/taxi_eventhub_connection_string.txt"
  content  = azurerm_eventhub_authorization_rule.taxi_auth.primary_connection_string
}