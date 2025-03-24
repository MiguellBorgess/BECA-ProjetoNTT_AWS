# 📊 ETL Project - Data Engineering Scholarship

## 📌 Description  
This repository contains the implementation of the **Final Project** for the **D&A Data Engineering Scholarship Program** by **NTT DATA Ltd.**. The project aims to explore and analyze the 2022 election data provided by TSE, utilizing **Big Data, ETL/ELT, and data visualization tools**.  

## 🏗️ Architecture and Technologies  
The solution was developed using the following technology stack:  

- **Storage**: AWS S3  
- **Data Processing**: AWS Glue, PySpark Scripts
- **Database**: AWS Athena
- **Visualization**: Power BI  
- **Code Versioning**: GitHub
  
## 📂 Repository Structure  

📦 integrated-project    
┣ 📂 terraform\t            # Project structure and deploy  
......┣ 📂 scripts\t           # PySpark scripts for ETL/ELT  
......┣ 📂 querys\t            # PySpark scripts for ETL/ELT\
┣ 📂 results\t              # Power BI files and Final powerpoint  
┣ 📂 docs\t                 # Project documentation and study\
┣ 📜 README.md\t            # This document  

## 📊 Analyses Conducted  
✅ General election statistics  
✅ Trends in candidate and party financial disclosures  
✅ Distribution of candidates across states and municipalities \
✅ Relationship between gender, race, and the level of government financial support for candidates\
✅ Analysis of the impact of elections on overall retail sales

## 🚧 Missing Features  
These features were not included in the project due to the strict deadline and the knowledge limitations we had at the time of implementation. However, they remain essential for future enhancements:
- **Crawler Creation**: Automating the data catalog.  
- **Data Validation**: Ensuring data quality and consistency throughout the pipeline.  
- **Silver to Gold Transformation**: Refining and enriching datasets for advanced analytics.
