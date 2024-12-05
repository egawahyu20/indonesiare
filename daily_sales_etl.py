# %%
import os
import re
import duckdb
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("daily_sales_etl.log"),
        logging.StreamHandler()
    ]
)

def standardize_column_names(df):
    """
    Standarisasi nama kolom dalam DataFrame.
    
    1. Mengubah huruf menjadi kecil.
    2. Menghapus spasi di awal atau akhir.
    3. Mengganti spasi dan karakter khusus dengan garis bawah (_).
    4. Menghapus karakter non-alfanumerik kecuali garis bawah.
    
    Args:
        df (pd.DataFrame): DataFrame yang kolomnya akan distandarisasi.
    
    Returns:
        df (pd.DataFrame): DataFrame dengan nama kolom yang sudah distandarisasi.
    """
    
    def clean_column(col):
        col = col.strip()
        col = col.lower()
        col = re.sub(r'[^0-9a-zA-Z\s_]', '', col)
        col = re.sub(r'\s+', '_', col)
        return col

    df.columns = [clean_column(col) for col in df.columns]
    return df

def extract(file_path):
    """
    Melakukan extract data dari csv
    
    1. Membaca file csv ke dalam dataframe.
    2. Melakukan standarisasi kolom.
    
    Args:
        file_path (String): File path dari csv yang ingin di import.
    
    Returns:
        df (pd.DataFrame): DataFrame yang di extract dan yang sudah distandarisasi.
    """
    try:
        logging.info("Extract started")
        df = pd.read_csv(file_path)
        df = standardize_column_names(df)
        logging.info("Extract succesfully")
        return df
    except Exception as e:
        logging.error(f"Extract failed, something went wrong: {e}")

def transform(df):
    """
    Melakukan transformasi data
    
    1. Menghapus baris yang memiliki nilai null di kolom price.
    2. Melakukan kalkulasi untuk kolom total_revenue.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan di transformasi.
    
    Returns:
        df (pd.DataFrame): DataFrame yang sudah dihilangkan nilai null dan juga menambahkan kolom baru.
    """
    try:
        logging.info("Transform started")
        df_transform = df.dropna(subset=["price"]).copy()
        df_transform["total_revenue"] = df_transform["quantity"] * df_transform["price"]
        logging.info("Transform succesfully")
        return df_transform
    except Exception as e:
        logging.error(f"Transform failed, something went wrong: {e}")

def load(df, output_file_path):
    """
    Melakukan Loading data ke csv dan juga duckdb
    
    1. Melakukan export ke csv.
    2. Menyimpan data di duckcb.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan di transformasi.
        output_file_path (String): Path file untuk menyimpan csv.
    """
    try:
        logging.info("Load started")
        df = df.reset_index(drop=True)

        logging.info("Export to csv started")
        df.to_csv(output_file_path, index=False)
        logging.info(f"file export successful. file is in {output_file_path}")

        logging.info("Load to duckdb started")
        _current_path = os.getcwd()
        duckdb_file_path = f"{_current_path}/indonesiare.duckdb"
        con = duckdb.connect(duckdb_file_path) 
        con.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales (
            transaction_id VARCHAR PRIMARY KEY,
            product_id VARCHAR,
            quantity INTEGER,
            price DECIMAL,
            transaction_date DATE,
            total_revenue DECIMAL
        )
        """)
        existing_transaction = con.execute("SELECT transaction_id FROM daily_sales").fetchall()
        existing_transaction = {row[0] for row in existing_transaction}

        new_data = df[~df["transaction_id"].isin(existing_transaction)]

        if not new_data.empty:
            con.execute("INSERT INTO daily_sales SELECT * FROM new_data")
            logging.info("New data has been saved.")
        else:
            logging.info("There is no new data to be save.")
        con.close()
        logging.info(f"Load to duckdb successful. file is in {duckdb_file_path}")        

        logging.info("Load succesfully")
        return df
    except Exception as e:
        logging.error(f"Load failed, something went wrong: {e}")

# %%
if __name__ == '__main__':
    current_path = os.getcwd()
    file_path = f"{current_path}/data/DE_daily_sales.csv"
    output_file_path = f"{current_path}/output/daily_sales_cleaned.csv"
    df = extract(file_path)
    df = transform(df)
    df = load(df, output_file_path)