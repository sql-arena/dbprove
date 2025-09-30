# SQL Server on port 1433
choco install sql-server-express --params "'/INSTANCENAME:SQLEXPRESS /TCPENABLED:1 /SECURITYMODE:SQL /SAPWD:password'"
choco install msodbcsql17
choco install sqlcmd -y

# PostgreSQL on port 5432
choco install postgresql --params '"/Password:password /Port:5432 /AllowRemote"'

# MariaDB on port 3306
choco install mariadb.install

# ClickHouse on port 9000
choco install clickhouse

