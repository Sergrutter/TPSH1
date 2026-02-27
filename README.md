# Telegram SQL Bot (GigaChat + PostgreSQL)

Это Telegram-бот, который принимает текстовые вопросы на русском языке, превращает их в SQL-запрос с помощью GigaChat и выполняет этот запрос в PostgreSQL. В ответ бот возвращает одно число из базы данных.

## Что делает проект

* Принимает текстовый запрос от пользователя
* Отправляет его в GigaChat
* Получает SQL (только SELECT)
* Выполняет запрос в PostgreSQL
* Возвращает пользователю результат

## Архитектура 

1. Пользователь пишет боту в Telegram
2. Бот отправляет текст в GigaChat вместе с системным промптом
3. GigaChat возвращает SQL-запрос
4. Бот проверяет, что это SELECT
5. Выполняет запрос через asyncpg
6. Отправляет число пользователю

## Структура базы данных

В базе есть 2 таблицы:

### videos

* id (UUID, primary key)
* creator_id (TEXT)
* video_created_at (TIMESTAMPTZ)
* views_count (INTEGER)
* likes_count (INTEGER)
* comments_count (INTEGER)
* reports_count (INTEGER)
* created_at (TIMESTAMPTZ)
* updated_at (TIMESTAMPTZ)

### video_snapshots

* id (UUID, primary key)
* video_id (UUID → videos.id)
* views_count
* likes_count
* comments_count
* reports_count
* delta_views_count
* delta_likes_count
* delta_comments_count
* delta_reports_count
* created_at
* updated_at

Связь таблиц:
videos.id = video_snapshots.video_id

## Как работает генерация SQL

В системном промпте модели:

* Описана схема таблиц
* Разрешён только SELECT
* Запрос должен возвращать одно число
* Запрещены INSERT, UPDATE, DELETE, DROP
* SUM всегда оборачивается в COALESCE(..., 0)
* Всегда указывается FROM
* Запрос заканчивается точкой с запятой

Модель возвращает только SQL без пояснений.

## Работа с GigaChat

Используется официальная библиотека gigachat.

* В код передаётся Authorization Key
* Библиотека сама получает access_token
* Токен автоматически обновляется каждые 30 минут

## Что есть в репозитории

* Исходный код бота (директория `bot/`)
* Backend-логика
* Скрипт загрузки JSON в БД (`load_data.py`)
* SQL-скрипты или создание таблиц из кода
* `docker-compose.yml`
* `.env_example`
* `README.md`

## Как запустить локально

1. Клонировать репозиторий
2. Создать файл `.env` на основе `.env_example`
3. Заполнить переменные
4. Выполнить:

```bash
docker compose up --build
```

После запуска:

* Поднимается PostgreSQL
* Загружаются данные из JSON
* Стартует Telegram-бот

## Какие данные нужны

В `.env`:

* `BOT_TOKEN` — токен Telegram-бота
* `GIGACHAT_API_KEY` — Authorization Key для GigaChat
* `DB_HOST`
* `DB_PORT`
* `DB_NAME`
* `DB_USER`
* `DB_PASSWORD`

### Как получить BOT_TOKEN

* Открыть @BotFather в Telegram
* Ввести `/newbot`
* Следовать инструкции
* Скопировать токен

### Как получить GIGACHAT_API_KEY

* Зарегистрироваться в кабинете разработчика Сбера
* Создать проект
* Получить Authorization Key

В `.env_example` указываются только названия переменных без реальных значений.

## Загрузка JSON в базу

В проекте есть скрипт:

* `load_data.py`
* Подключается к PostgreSQL
* Создаёт таблицы 
* Загружает данные из `videos.json`
