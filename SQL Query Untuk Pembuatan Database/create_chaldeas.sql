-- Created by GitHub Copilot in SSMS - review carefully before executing
CREATE DATABASE [chaldeas]
ON PRIMARY (
    NAME = N'chaldeas_data',
    FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\chaldeas.mdf',
    SIZE = 50MB,
    FILEGROWTH = 10MB
)
LOG ON (
    NAME = N'chaldeas_log',
    FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\chaldeas_log.ldf',
    SIZE = 20MB,
    FILEGROWTH = 10MB
);