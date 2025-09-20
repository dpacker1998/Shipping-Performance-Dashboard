import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine, text

def generate_date_dim(start_year=2015, end_year=2035):
    start = pd.to_datetime(f"{start_year}-01-01")
    end = pd.to_datetime(f"{end_year}-12-31")

    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"full_date": dates})

    # surrogate key YYYYMMDD
    df["date_sk"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)

    # weekday: Monday=0 ... Sunday=6
    df["weekday"] = df["full_date"].dt.weekday

    # cal_week: week number, Sunday start
    df["cal_week"] = df["full_date"].dt.strftime("%U").astype(int)

    # iso_week: week number, Monday start
    iso = df["full_date"].dt.isocalendar()
    df["iso_week"] = iso["week"].astype(int)

    # month / quarter / year
    df["cal_month"] = df["full_date"].dt.month
    df["cal_qtr"] = df["full_date"].dt.quarter
    df["cal_year"] = df["full_date"].dt.year

    # week start/end (Sunday start for cal, Monday start for ISO)
    df["cal_wk_start_date"] = df["full_date"] - pd.to_timedelta((df["weekday"] + 1) % 7, unit="D")
    df["cal_wk_end_date"] = df["cal_wk_start_date"] + timedelta(days=6)

    df["iso_wk_start_date"] = df["full_date"] - pd.to_timedelta(df["weekday"], unit="D")
    df["iso_wk_end_date"] = df["iso_wk_start_date"] + timedelta(days=6)

    # -----------------------------
    # Extra descriptive columns
    # -----------------------------
    df["cal_month_short_name"] = df["full_date"].dt.strftime("%b")     # Jan, Feb, ...
    df["cal_month_long_name"] = df["full_date"].dt.strftime("%B")      # January, February, ...
    
    # Sunday=1 ... Saturday=7
    df["cal_day_of_week_num"] = df["full_date"].dt.dayofweek.apply(lambda x: (x + 1) % 7 + 1)

    # Monday=1 ... Sunday=7 (ISO standard)
    df["iso_day_of_week_num"] = df["full_date"].dt.isocalendar().day

    df["day_of_week_short_name"] = df["full_date"].dt.strftime("%a")  # Mon, Tue, ...
    df["day_of_week_long_name"] = df["full_date"].dt.strftime("%A")   # Monday, Tuesday, ...

    # keep only required columns
    date_dim = df[
        [
            "date_sk",
            "full_date",
            "cal_week",
            "iso_week",
            "cal_month",
            "cal_qtr",
            "cal_year",
            "cal_wk_start_date",
            "iso_wk_start_date",
            "cal_wk_end_date",
            "iso_wk_end_date",
            "cal_month_short_name",
            "cal_month_long_name",
            "cal_day_of_week_num",
            "iso_day_of_week_num",
            "day_of_week_short_name",
            "day_of_week_long_name",
        ]
    ].copy()

    # convert datetime64 to plain date objects
    for col in [
        "full_date",
        "cal_wk_start_date",
        "iso_wk_start_date",
        "cal_wk_end_date",
        "iso_wk_end_date",
    ]:
        date_dim[col] = pd.to_datetime(date_dim[col]).dt.date

    return date_dim


def write_date_dim_to_postgres(date_dim, db_user, db_pass, db_host, db_port, db_name,
                               schema="brimich_logistics", table="date_dim", if_exists="replace"):
    """Write date_dim dataframe into PostgreSQL"""
    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
    date_dim.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False)
    print(f"Wrote {len(date_dim)} rows into {schema}.{table}")


if __name__ == "__main__":
    # Step 1: generate calendar
    date_dim = generate_date_dim(2015, 2035)

    # Step 2: write to PostgreSQL (update these values!)
    DB_CREDENTIALS = {
        "db_user": "postgres",
        "db_pass": "dGp1998rJs1989!",
        "db_host": "localhost",
        "db_port": "5432",
        "db_name": "postgres"
    }

    write_date_dim_to_postgres(date_dim, **DB_CREDENTIALS)