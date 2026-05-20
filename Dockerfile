FROM apache/airflow:3.2.1

# Install project runtime dependencies into Airflow's Python environment.
# src/ is bind-mounted at runtime, so code changes don't require a rebuild.
USER airflow
RUN pip install --no-cache-dir \
    "aiohttp>=3.13.5" \
    "aiolimiter>=1.2.1" \
    "click>=8.4.0" \
    "cryptography>=48.0.0" \
    "duckdb>=1.5.2" \
    "matplotlib>=3.10.9" \
    "polars>=1.40.1" \
    "pyarrow>=24.0.0" \
    "python-dotenv>=1.2.2" \
    "requests>=2.34.2" \
    "scipy>=1.17.1" \
    "plotly>=5.24.0" \
    "seaborn>=0.13.2" \
    "statsmodels>=0.14.0" \
    "tqdm>=4.67.3"
