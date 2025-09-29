MySQL Backup and Rollback

This document describes how to back up the `listing` table (and other tables) from the local MySQL used by this project and how to restore if needed.

Prerequisites:
- `mysqldump` available (from MySQL client tools)
- Connection details: host, port, user, password, database (defaults in project: host=127.0.0.1 port=33062 user=pyscrape password=pyscrape_pwd database=pyscrape)

Backup example (UNIX / WSL / Git Bash):

```bash
mysqldump -h127.0.0.1 -P33062 -upyscrape -ppyscrape_pwd pyscrape listing > listing_$(date +%Y%m%d_%H%M%S).sql
```

Windows PowerShell example:

```powershell
mysqldump -h127.0.0.1 -P33062 -upyscrape pyscrape listing > listing_$(Get-Date -Format yyyyMMdd_HHmmss).sql
```

To back up the whole database:

```bash
mysqldump -h127.0.0.1 -P33062 -upyscrape -ppyscrape_pwd pyscrape > pyscrape_full_$(date +%Y%m%d_%H%M%S).sql
```

Rollback / restore example:

```bash
mysql -h127.0.0.1 -P33062 -upyscrape -ppyscrape_pwd pyscrape < listing_20250101_120000.sql
```

Notes:
- Always verify the dump file exists and is non-empty before restoring.
- Consider stopping writers or taking a brief maintenance window when doing restores to avoid write conflicts.
