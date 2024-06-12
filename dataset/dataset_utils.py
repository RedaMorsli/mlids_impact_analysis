import pandas as pd
import metadata as md
import os


def load_merged_dataset_by_attack_flow_based(attack_data):
    flow_df = load_flow_data_by_attack(attack_data)
    res_df = load_res_data_by_attack(attack_data)

    flow_df['timestamp'] = pd.to_datetime(flow_df['timestamp'])
    res_df['timestamp'] = pd.to_datetime(res_df['timestamp'])
    flow_df = flow_df.sort_values(by='timestamp').reset_index(drop=True)
    res_df = res_df.sort_values(by='timestamp').reset_index(drop=True)

    merged_df = pd.merge_asof(flow_df, res_df, on='timestamp', direction='nearest', tolerance=pd.Timedelta("5s"))

    return merged_df


def load_flow_data_by_attack(attack_data: dict, log=True):
    path = attack_data["path"] + os.sep + md.NET_FLOWS_FILE_NAME + ".csv"
    df = pd.read_csv(path)

    # Filter by time
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[(df['timestamp'] >= attack_data["start_time"]) & (df['timestamp'] <= attack_data["end_time"])]

    # Replace class values ([0] -> 0 and [1] -> 1)
    df.loc[df["class"] == "[0]", 'class'] = 0
    df.loc[df["class"] == "[1]", 'class'] = 1

    if log:
        print(f"{path} successfully loaded")

    return df


def load_res_data_by_attack(attack_data: dict, log=True):
    resource_dfs = []

    for resource_file_name in md.RES_FILE_NAMES.values():
        path = attack_data["path"] + os.sep + resource_file_name + ".csv"
        df = pd.read_csv(path)

        # Remove undesirable cols
        if "Value 1" in df.columns:
            df.drop(columns=["Value 1"], inplace=True)

        # Change cols names
        if "Time" in df.columns:
            df.rename(columns={"Time": "timestamp"}, inplace=True)

        if len(df.columns) > 2:
            summed_col_name = resource_file_name + "_sum"
            df[summed_col_name] = df.drop(columns=["timestamp"]).sum(axis=1)
            df = df.get(["timestamp", summed_col_name])
        else:
            df.rename(columns={df.columns[1]: resource_file_name}, inplace=True)

        if log:
            print(f"{path} successfully loaded")

        resource_dfs.append(df)

    # Merge resource data
    merged_df = pd.merge(resource_dfs[0], resource_dfs[1], on='timestamp')
    for df in resource_dfs[2:]:
        merged_df = pd.merge(merged_df, df, on='timestamp')

    return merged_df
