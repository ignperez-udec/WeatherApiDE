# WeatherApiDE 🌤️

**WeatherApiDE** is a complete Data Engineering project that extracts, processes, and stores weather data using a modern, containerized architecture.

## 📌 Overview

This project performs:

- Historical and daily weather data extraction  
- Bronze/Silver data processing layers (Gold in development)
- PostgreSQL storage  
- Automated ETL orchestration with **Apache Airflow**  
- Data visualization using **Metabase** (In development)
- Fully containerized setup with **Docker Compose**

---

## 📁 Project Structure

```
WeatherApiDE/
│
├── airflow/              
│   ├── dags/    
│       └── src/          
│   ├── logs/             
│   └── plugins/ 
│ 
├── metabase/               
│   ├── Dockerfile
│   └── start_metabase.sh  
│ 
├── python/               
│   ├── Dockerfile
│   ├── requirements.txt
│   └── init.sh           
│
├── src/                  
│   ├── bronze/           
│   ├── silver/           
│   └── gold/                   
│
├── data/                
│
├── docker-compose.yml    
├── .gitignore            
├── README.md             
└── .env.example          
```

---

## 🛠️ Technologies

- Python 3.12
- Apache Airflow 2.10.1 
- PostgreSQL 15
- Metabase 0.50.10 (In development)
- Docker & Docker Compose  
- pandas, SQLAlchemy, psycopg2, requests, pyarrow, lxml  

---

## 📦 Requirements

- Docker  
- Docker Compose  
- Git  

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/ignperez-udec/WeatherApiDE.git
cd WeatherApiDE
```

### 2. Create `.env` from the example file

```bash
cp .env.example .env
```

Then fill required variables:

```
#Database
DB_USER=ChangeUser
DB_PASSWORD=ChangePass

#Metabase
METABASE_USER=Change@email.com
METABASE_PASSWORD=Change

#Airflow
AIRFLOW_DB_USER=ChangeUser
AIRFLOW_DB_PASSWORD=ChangePass
AIRFLOW_ADMIN_USERNAME=ChangeUser
AIRFLOW_ADMIN_PASSWORD=ChangePass
AIRFLOW_ADMIN_FIRSTNAME=ChangeName
AIRFLOW_ADMIN_LASTNAME=ChangeLastName
AIRFLOW_ADMIN_EMAIL=Change@email.com

#SMTP
AIRFLOW__SMTP__SMTP_USER=Sender@email.com
AIRFLOW__SMTP__SMTP_PASSWORD=AppPasswordSenderEmail
AIRFLOW__SMTP__SMTP_MAIL_FROM=Recipient@email.com
```

### 3. Start the stack

```bash
docker compose up --build
```
The command initializes technologies and run init.py script. This script extracts locations from Wikipedia and loads historical weather for the locations from API_LOCATIONS_TO_EXTRACT variable (default "8101,2101,5101,13101,10101"). The location codes to extract can be changed in the src/variables.json file.

### 4. Access dashboards & tools

| Service       | URL |
|---------------|-----|
| Airflow UI    | http://localhost:8080 |
| Metabase      | http://localhost:3000 |
| PostgreSQL    | localhost:5432 |

---

## ✨ Airflow DAG: `daily_weather_etl`

This DAG automates the daily ETL:

- Reads last weather date  
- Extracts pending weather records  
- Loads fresh data into PostgreSQL  
- Sends email alerts if configured  

**Key DAG features:**

- `max_active_runs = 1` (only one run at a time)
- Retries with delay
- `depends_on_past = False`
- Daily schedule  

---

## 📄 License

MIT License.

---

Thanks for checking out the project! 🚀🌦️
