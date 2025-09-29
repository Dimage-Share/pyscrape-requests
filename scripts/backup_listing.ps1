# Backup listing table (PowerShell)
$host = $env:MYSQL_HOST -or '127.0.0.1'
$port = $env:MYSQL_PORT -or '33062'
$user = $env:MYSQL_USER -or 'pyscrape'
$pass = $env:MYSQL_PASSWORD -or 'pyscrape_pwd'
$db = $env:MYSQL_DATABASE -or 'pyscrape'
$ts = (Get-Date).ToString('yyyyMMdd_HHmmss')
$fn = "listing_$ts.sql"

# Run mysqldump (assumes mysqldump is in PATH)
$env:MYSQL_PWD = $pass
mysqldump -h$host -P$port -u$user $db listing > $fn
Remove-Item Env:MYSQL_PWD
Write-Output "Dump written to $fn"
