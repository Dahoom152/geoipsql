import pandas as pd

def read_and_extract(file_path):
    df = pd.read_csv(file_path)
    df = df[['network', 'country_code', 'state', 'city']]
    return df

def write_to_sql(df, output_file):
    with open(output_file, 'w') as f:
        for _, row in df.iterrows():
            f.write(f"INSERT INTO ipv4 (network, country_code, state, city) VALUES ('{row['network']}', '{row['country_code']}', '{row['state']}', '{row['city']}');\n")

if __name__ == "__main__":
    df = read_and_extract('geolocationDatabaseIPv4.csv')
    write_to_sql(df, 'db.sql')