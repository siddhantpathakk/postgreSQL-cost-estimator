# Costly Calculations

A Deep Dive into Cost Estimation Algorithms for Optimised Query Execution in PostgreSQL

Please note that this project currently only focuses and is adapted on the TPC-H dataset.

## Features

- User-friendly interactive interface for ease of interaction
- Easy to understand and interpret visualisations for Query Execution Plans for your SQL queries
- See query outputs in real time with almost zero latency, directly brought to you by PostgreSQL
- Cross platform - compatible with all Windows, Mac and Linux users.

## Acknowledgements

We express our gratitude to Associate Professor Sourav Saha Bhowmick for his invaluable guidance, support and encouragement throughout the duration of the course CZ4031: Database System Principles, offered by the School of Computer Science and Engieering at NTU.

We would also like to mention some resources we found really interesting in clearing our concepts about Query Execution Plans and cost estimation based mathematics in PostgreSQL

- [PostgreSQL Documentation](https://www.postgresql.org/docs/current/using-explain.html)
- [PostgreSQL Github Repository (Mirror)](https://github.com/postgres/postgres/tree/82023d47de9e262730b1f9b4ea77fae201a89d0a)
- [ARENA: Alternative Relational Query Plan Exploration for Database Education](https://dbedu.xidian.edu.cn/arena/)

## Tech Stack

Streamlining with the project's guidelines, we have decided to use a full Python-based framework. Below is a summarised list of main Python libraries used in developing it.

## Ensure PGadmin4 is already running and connected to PostgreSQL

**Client:** Gradio, Plotly

**Server:** Psycopg, NumPy, Pydantic

## Run Locally

Go to the project directory

```bash
  cd src
```

Install dependencies (preferably in a virtual environment, using Anaconda or Venv)

```bash
  conda create --name postgres-qep-venv python=3.10
  conda activate postgres-qep-venv
  pip install -r requirements.txt
```

If running via CMD(Administrator)

```bash
  python3 -m venv ./venv
  .\venv\Scripts\activate.bat
  pip install -r requirements.txt
```

Start the server and launch the web application on a new browser tab

```bash
  python project.py
```

## Authors

- [@siddhantpathakk](https://www.github.com/siddhantpathakk)
- [@deadinside1880](https://www.github.com/deadinside1880)
- [@adrian-ang97](https://www.github.com/adrian-ang97)
- [@Fatalityzx](https://www.github.com/Fatalityzx)
