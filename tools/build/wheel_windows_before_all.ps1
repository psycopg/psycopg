# Start PostgreSQL service for test
$PgSvc = Get-Service "postgresql*"
Set-Service $PgSvc.Name -StartupType manual
$PgSvc.Start()
