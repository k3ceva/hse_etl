# Конвейер ETL для аналитики событий GitHub

Этот репозиторий содержит пайплайны для инкрементального извлечения, преобразования и загрузки событий GitHub из MongoDB в PostgreSQL и формирования двух аналитических витрин.

## Настройка инфраструктуры

Инфраструктура состоит из MongoDB, работающей на виртуальной машине, и PostgreSQL, работающей в Docker Compose:

- **MongoDB**: Установлен на виртуальной машине CentOS из официального репозитория MongoDB

```bash
$ sudo systemctl status mongod
● mongod.service - MongoDB Database Server
   Loaded: loaded (/usr/lib/systemd/system/mongod.service; enabled; vendor preset: disabled)
  Drop-In: /etc/systemd/system/mongod.service.d
           └─restart.conf
   Active: active (running) since Sun 2026-03-08 09:59:35 UTC; 3h 14min ago
     Docs: https://docs.mongodb.org/manual
 Main PID: 1130 (mongod)
    Tasks: 30
   Memory: 6.9G
   CGroup: /system.slice/mongod.service
           └─1130 /usr/bin/mongod -f /etc/mongod.conf
```

- **PostgreSQL**: Работает в контейнере Docker, управляемом через docker-compose

```bash
$ sudo docker compose logs postgres | head -n 20
airflow_postgres  | 
airflow_postgres  | PostgreSQL Database directory appears to contain a database; Skipping initialization
airflow_postgres  | 
airflow_postgres  | 2026-03-08 08:10:11.210 UTC [1] LOG:  starting PostgreSQL 13.23 (Debian 13.23-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
airflow_postgres  | 2026-03-08 08:10:11.210 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
airflow_postgres  | 2026-03-08 08:10:11.210 UTC [1] LOG:  listening on IPv6 address "::", port 5432
airflow_postgres  | 2026-03-08 08:10:11.219 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
```

## Загрузка событий из MongoDB в PostgreSQL

Конвейер github_etl извлекает события GitHub PushEvent и PullRequestEvent из коллекции MongoDB (`github_history`) и загружает их в БД. Процесс загружает инкремент данных за час с использованием переменных Airflow `data_interval_start` и `data_interval_end`

### Преобразование данных

Конвейер преобразует необработанные данные событий GitHub, извлекая только поля, необходимые для аналитики:

#### События Push

- `created_at`: Время возникновения push-события
- `actor`: Пользователь, выполнивший push
- `repo`: Репозиторий, в который был сделан push
- `ref`: Ссылка в Git (ветка), в которую был сделан push
- `commit_count`: Количество коммитов в push-событии (рассчитывается в MongoDB для сокращения объема данных и передачи вычислительной нагрузки из Airflow)

#### События Pull Request

- `created_at`: Время возникновения события PR
- `actor`: Пользователь, выполнивший действие
- `repo`: Репозиторий, в котором находится PR
- `pr_number`: Номер Pull Request
- `action`: Тип действия

Обработка двух типов событий происходит параллельно в тасках push_etl_task и pr_etl_task: заполяются таблицы push_events и pull_request_events.
Для избежания возникновения дубликатов у таблиц были созданы первичныме ключи по полям created_at, actor, repo. В случае, если запись с таким ключом уже есть, то она обновляется:

```sql
ON CONFLICT (created_at, actor, repo)
DO UPDATE SET
    pr_number = EXCLUDED.pr_number,
    action = EXCLUDED.action
```

## Формирование аналитических витрин PostgreSQL

Витрины формируются ежедневно, содержат агрегированные данные по пользователям и репозиториям. Загрузка осуществляется через временные партиции, вставляемые в целевые таблицы витрин. Витрины партиционированы по отчетной дате event_date - на каждую дату своя партиция

### Витрина активности пользователей (`dm.user_activity`)

- `event_date`: Отчетная дата
- `actor`: Пользователь GitHub
- `push_count`: Количество push-событий
- `pr_count`: Количество взаимодействий с Pull Request
- `total_commits`: Общее количество запушенных коммитов
- `last_activity`: Отметка времени последней активности

### Витрина статистики репозитория (`dm.repository_stats`)

- `event_date`: Отчетная дата
- `repo`: Имя репозитория
- `total_pushes`: Количество push-событий
- `total_commits`: Общее количество коммитов
- `last_push`: Отметка времени последнего пуша в репозиторий
- `unique_contributors`: Количество 
- `total_pr_opened`: Количество открытых PR
- `last_pr_opened_number`: Номер последнего открытого PR
- `last_pr_opened_at`: Отметка времени последнего открытого PR
- `total_pr_closed`: Количество закрытых PR
- `last_pr_closed_number`: Номер последнего закрытого PR
- `last_pr_closed_at`: Отметка времени последнего закрытого PR


### Пример данных

С неполными выгрузками можно ознакомится в файлах `repository_stats.txt` и `user_activity.txt`

