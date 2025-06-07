# msodbc driver system

This driver covers the numerous, database engines created by the warring factions inside Microsoft.

It can connect to:

- SQL Server
- Azure Fabric Warehouse

Since Microsoft does not distribute sources for connectivity, it relies on the `msodbcsql<version>.dll/so` to
be available on the building machine. 

In the case of Azure Fabric, there is no direct path via FreeTDS because it relies on authentication headers special to 
Azure. Hence, we have to fall back to vanilla ODBC.

