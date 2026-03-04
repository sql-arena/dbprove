FROM mcr.microsoft.com/mssql/server:2022-latest

# Accept EULA and set default edition to Developer
ENV ACCEPT_EULA=Y
ENV MSSQL_PID=Developer

# Copy benchmark configuration
COPY conf/mssql.conf /var/opt/mssql/mssql.conf

# Expose SQL Server port
EXPOSE 1433
