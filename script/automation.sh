#!/bin/bash
# -------------------------------
# TMDB ETL Pipeline Runner
# -------------------------------

# Navigate to your project directory
cd Desktop/Volts/week7/envp_movie_etl


# Run the ETL pipeline
python3 scr/etl_pipeline.py

# Log completion to a global daily log
echo "ETL run completed on $(date)" >> logs/daily_run_summary.log