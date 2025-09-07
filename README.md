# Jobsites-ETL 🚀

An **ETL pipeline** built with **Apache Airflow (Astro)** that scrapes job listings from multiple job sites, integrates them, and loads them into a **PostgreSQL (AWS RDS)** database for downstream analytics.

This project automates job data collection from [Jobberman](https://www.jobberman.com) and [MyJobMag](https://www.myjobmag.com), integrates both sources, and ensures deduplicated, clean storage. Notifications are sent after DAG execution for success/failure tracking.

---

## 📂 Project Structure

```bash
Jobsites-ETL/
├── dags/
│   └── job_sites_dag.py         # Main Airflow DAG definition
├── include/
│   ├── extension.py              # Utility decorator for scraping sites
│   ├── jobberman.py              # Jobberman scraper
│   ├── myjobmag.py               # MyJobMag scraper
│   ├── integrate.py              # Integration logic (merge datasets)
│   ├── load_to_postgres.py       # Loader for PostgreSQL (deduplicated inserts)
├── requirements.txt              # Project dependencies
---

## ⚙️ Tech Stack

- **Apache Airflow (Astronomer Runtime)** – workflow orchestration  
- **Python** – ETL logic (scraping, parsing, integration, loading)  
- **BeautifulSoup4** – HTML parsing & web scraping  
- **Pandas** – dataset integration and transformation  
- **PostgreSQL (AWS RDS)** – storage for job listings  
- **Airflow Providers** – `PostgresHook`, `HttpSensor`, `EmailOperator`  
- **SMTP** – DAG run notifications  

---

## 📊 ETL Workflow (DAG)

1. **HttpSensors**  
   - Wait for Jobberman and MyJobMag endpoints to be available  

2. **Extract**  
   - `extract_jobberman()` → Scrapes Jobberman job listings  
   - `extract_myJobMag()` → Scrapes MyJobMag job listings  

3. **Transform**  
   - `integrate_results()` → Combines both datasets into a unified record set  

4. **Load**  
   - `load_to_postgres()` → Inserts jobs into PostgreSQL (`job_listings` table)  
   - Uses **ON CONFLICT DO NOTHING** to avoid duplicates based on `href` (job link)  

5. **Notify**  
   - `sendEmailForFailureOrSuccess` → Sends an email after DAG run (success or failure)  

---

## 🗂️ Module Breakdown

### `include/extension.py`
- Contains `connectToJobSite` decorator  
- Handles HTTP requests, retries, and parsing with **BeautifulSoup**  

### `include/jobberman.py`
- Scraper for Jobberman job listings  
- Extracts title, company, posted date, location, and job URL  

### `include/myjobmag.py`
- Scraper for MyJobMag job listings (multiple pages)  
- Follows links into job detail pages for richer metadata  

### `include/integrate.py`
- Combines Jobberman + MyJobMag records into a single dataset  

### `include/load_to_postgres.py`
- Loads integrated data into **AWS RDS Postgres**  
- Deduplication handled via `ON CONFLICT (href) DO NOTHING`  

---

## 🗄️ Database Schema

The DAG creates the target table if it doesn’t exist:

```sql
CREATE TABLE IF NOT EXISTS job_listings (
    href TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    posted_at TEXT NOT NULL,
    location TEXT NOT NULL,
    source TEXT NOT NULL
);
