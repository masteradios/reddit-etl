# Reddit Data Engineering Project 🚀

A data engineering project designed to build a robust data pipeline for collecting, processing, and storing Reddit data efficiently.

![image](https://github.com/user-attachments/assets/8d169d19-8627-4529-8dab-64ef62b7f273)

## Tools & Technologies Used 🛠️
- **Apache Airflow**: Orchestrated and automated the entire data pipeline, managing data flow and task scheduling.
- **Python**: Used for data extraction, transformation, and loading (ETL) within Airflow workflows.
- **PRAW (Python Reddit API Wrapper)**: Integrated to fetch real-time Reddit posts and extract relevant data.
- **Pandas**: Handled data manipulation, cleaning, and structuring within Python scripts in Airflow tasks.
- **AWS S3**: Used as the data lake to store large volumes of Reddit data securely and cost-effectively.
- **AWS RDS (Amazon Relational Database Service)**: Configured PostgreSQL on AWS RDS for structured data storage and efficient querying.
- **PostgreSQL**: Stored structured Reddit data to support efficient querying and analysis.

## Project Highlights 📌
- Designed and implemented **Airflow DAGs (Directed Acyclic Graphs)** to define the workflow steps, task dependencies, and scheduling for data processing.
- Automated data extraction from multiple subreddits, capturing attributes like post titles, URLs, authors, scores, and engagement metrics.
- Performed data transformation and cleaning tasks within Airflow using Python scripts, ensuring data quality and consistency.
- Stored cleaned and structured Reddit data in **PostgreSQL** for analysis, reporting, and visualization.
- Monitored and managed the entire data pipeline using Airflow's dashboard, scheduling features, and error handling capabilities.




