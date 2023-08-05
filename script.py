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
    network = network.strip().rstrip(',')
    if '-' in network:
        start, end = network.split('-')
        start = int(ip.ip_network(start.strip(), strict=False).network_address)
        end = int(ip.ip_network(end.strip(), strict=False).network_address)
    else:
        ip_net = ip.ip_network(network.strip(), strict=False)
        start = int(ip_net.network_address)
        end = int(ip_net.network_address) + ip_net.num_addresses - 1

    return pd.Series([start, end])

def write_to_sql(df, output_file1, output_file2):
    half = len(df) // 2
    with gzip.open(output_file1, 'wt') as f1, gzip.open(output_file2, 'wt') as f2:
        f1.write("INSERT INTO your_table (network, start, end, country_code, state, city) VALUES ")
        f2.write("INSERT INTO your_table (network, start, end, country_code, state, city) VALUES ")

        insert_values = []
        for i, (_, row) in enumerate(df.iterrows()):
            insert_values.append(f"('{row['network']}',{row['start']},{row['end']},'{row['country_code']}','{row['state']}','{row['city']}')")
            if (i + 1) % 5000 == 0:
                if i < half:
                    f1.write(",".join(insert_values) + ",")
                else:
                    f2.write(",".join(insert_values) + ",")
                insert_values = []
        
        if insert_values:
            if i < half:
                f1.write(",".join(insert_values) + ";")
            else:
                f2.write(",".join(insert_values) + ";")

if __name__ == "__main__":
    df = read_and_extract('geolocationDatabaseIPv4.csv')
    write_to_sql(df, 'db1.sql.gz', 'db2.sql.gz')
