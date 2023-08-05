import pandas as pd
import gzip
import ipaddress as ip_network

def read_and_extract(file_path):
    df = pd.read_csv(file_path)
    df = df[['network', 'country_code', 'state', 'city']]
    
    df[['start', 'end']] = df['network'].apply(process_network).tolist()

    return df

def process_network(network):
    if '-' in network:
        start, end = network.split('-')
        start = int(ip_network(start.strip()).network_address)
        end = int(ip_network(end.strip()).network_address)
    else:
        ip_net = ip_network(network.strip())
        start = int(ip_net.network_address)
        end = int(ip_net.network_address) + ip_net.num_addresses - 1

    return pd.Series([start, end])

def write_to_sql(df, output_file):
    with gzip.open(output_file, 'wt') as f:
        f.write("INSERT INTO your_table (network, start, end, country_code, state, city) VALUES ")
        insert_values = []
        for _, row in df.iterrows():
            insert_values.append(f"('{row['network']}', {row['start']}, {row['end']}, '{row['country_code']}', '{row['state']}', '{row['city']}')")
        f.write(",\n".join(insert_values) + ";")

if __name__ == "__main__":
    df = read_and_extract('geolocationDatabaseIPv4.csv')
    write_to_sql(df, 'db.sql.gz')
