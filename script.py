import pandas as pd
import gzip
import ipaddress as ip

def read_and_extract(file_path):
    df = pd.read_csv(file_path)
    df = df[['network', 'country_code', 'state', 'city']]
    
    # Apply the process_network function on the DataFrame, not a Series
    df[['start', 'end']] = df.apply(lambda row: process_network(row['network']), axis=1, result_type='expand')

    return df

def process_network(network):
    if '-' in network:
        start, end = network.split('-')
        start = int(ip.ip_network(start.strip()).network_address)
        end = int(ip.ip_network(end.strip()).network_address)
    else:
        ip_net = ip.ip_network(network.strip())
        start = int(ip_net.network_address)
        end = int(ip_net.network_address) + ip_net.num_addresses - 1

    return pd.Series([start, end])

def write_to_sql(df, output_file):
    with gzip.open(output_file, 'wt') as f:
        f.write("INSERT INTO ipv4 (network, start, end, country_code, state, city) VALUES ")
        
        # Batch the writes to the file
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            batch = df[i:i + batch_size]
            insert_values = []
            for _, row in batch.iterrows():
                insert_values.append(f"('{row['network']}', {row['start']}, {row['end']}, '{row['country_code']}', '{row['state']}', '{row['city']}')")
            f.write(",\n".join(insert_values) + ";\n")
            if i + batch_size < len(df):
                f.write("INSERT INTO your_table (network, start, end, country_code, state, city) VALUES ")

if __name__ == "__main__":
    df = read_and_extract('geolocationDatabaseIPv4.csv')
    write_to_sql(df, 'db.sql.gz')
