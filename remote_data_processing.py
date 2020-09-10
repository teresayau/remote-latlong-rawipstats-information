import os
import pandas as pd
import sys
import re


def main():
    rawipstats_path, latlong_path = sys.argv[1:3]
    remote_data = RemoteDataProcessing()
    array_file_names_rawipstats = remote_data.get_array_file_names(
        rawipstats_path)
    array_file_names_latlong = remote_data.get_array_file_names(
        latlong_path)
    common_ids = remote_data.unique_common_ids(rawipstats_path,
                                               latlong_path)
    full_df_rawipstats = remote_data.df_all_remotes_rawipstats(common_ids,
                                                               array_file_names_rawipstats,
                                                               rawipstats_path)
    full_df_latlong = remote_data.df_all_remotes_latlong(common_ids,
                                                         array_file_names_latlong,
                                                         latlong_path)
    df_merged = remote_data.merge_rawipstats_latlong_dfs(full_df_rawipstats,
                                                         full_df_latlong)

    print(df_merged)


class RemoteDataProcessing:

    def unique_common_ids(self, rawipstats_path, latlong_path):
        array_file_names_rawipstats = self.get_array_file_names(rawipstats_path)
        array_file_names_latlong = self.get_array_file_names(latlong_path)
        array_remote_ids_rawipstats = self.get_array_remote_ids(
            array_file_names_rawipstats)
        array_remote_ids_latlong = self.get_array_remote_ids(
            array_file_names_latlong)
        set_remote_ids_rawipstats = self.get_set_remote_ids(
            array_remote_ids_rawipstats)
        set_remote_ids_latlong = self.get_set_remote_ids(array_remote_ids_latlong)
        common_ids = self.get_common_ids(set_remote_ids_rawipstats, set_remote_ids_latlong)

        return common_ids

    @staticmethod
    def get_array_file_names(folder_path):
        array_file_names = os.listdir(folder_path)

        return array_file_names

    @staticmethod
    def get_array_remote_ids(array_file_names):
        array_remote_ids = []
        for file_name in array_file_names:
            remote_id = re.findall(r'\d+', file_name)[0]
            array_remote_ids.append(remote_id)

        return array_remote_ids

    @staticmethod
    def get_set_remote_ids(array_remote_ids):

        return set(array_remote_ids)

    @staticmethod
    def get_common_ids(set_ids_rawipstats, set_ids_latlong):
        common_ids = set_ids_rawipstats.intersection(set_ids_latlong)

        return sorted(common_ids)

    @staticmethod
    def filter_files_by_remote_id(remote_id, array_file_names):
        array_filtered_files_to_read = []
        for file_name in array_file_names:
            file_remote_id = re.findall(r'\d+', file_name)[0]
            if file_remote_id == remote_id:
                array_filtered_files_to_read.append(file_name)

        return array_filtered_files_to_read

    @staticmethod
    def read_filtered_files_rawipstats(folder_path,
                                       array_filtered_files_to_read):
        array_dfs = []
        column_names = ['Datetime', 'TxTCP', 'RxTCP',
                        'TxUDP', 'RxUDP', 'TxICMP',
                        'RxICMP', 'TxIGMP', 'RxIGMP',
                        'TxHTTP', 'RxHTTP', 'TxOTHER', 'RxOTHER']
        for file_name in array_filtered_files_to_read:
            file_path = os.path.join(folder_path + file_name)
            df = pd.read_csv(file_path, names=column_names,
                             skip_blank_lines=True)
            array_dfs.append(df)
        df_rawipstats = pd.concat(array_dfs, ignore_index=True)

        return df_rawipstats

    @staticmethod
    def read_filtered_files_latlong(folder_path,
                                    array_filtered_files_to_read):
        array_dfs = []
        column_names = ['Datetime', 'Latitude',
                        'Longitude', 'Altitude']
        for file_name in array_filtered_files_to_read:
            file_path = os.path.join(folder_path + file_name)
            df = pd.read_csv(file_path, names=column_names,
                             skip_blank_lines=True)
            array_dfs.append(df)
        df_latlong = pd.concat(array_dfs, ignore_index=True)

        return df_latlong

    @staticmethod
    def process_df_rawipstats(df_rawipstats):
        df_rawipstats['Datetime'] = pd.to_datetime(df_rawipstats['Datetime'],
                                                   format='%Y-%m-%d %H:%M:%S')
        df_rawipstats['Datetime'] = df_rawipstats['Datetime'].dt.round('10min')
        total_upload = df_rawipstats.loc[:, ['TxTCP', 'TxUDP', 'TxICMP',
                                             'TxIGMP', 'TxHTTP', 'TxOTHER']].sum(axis=1)
        total_download = df_rawipstats.loc[:, ['RxTCP', 'RxUDP', 'RxICMP',
                                               'RxIGMP', 'RxHTTP', 'RxOTHER']].sum(axis=1)
        df_rawipstats['Total UP'] = total_upload
        df_rawipstats['Total Down'] = total_download
        column_grouped_by = ['Datetime']
        df_grouped = df_rawipstats.groupby(column_grouped_by).agg(
            {'Total UP': 'sum', 'Total Down': 'sum'})

        return df_grouped.reset_index()

    @staticmethod
    def process_df_latlong(df_latlong):
        df_latlong['Datetime'] = pd.to_datetime(df_latlong['Datetime'],
                                                format='%Y-%m-%d %H:%M:%S')
        df_latlong['Datetime'] = df_latlong['Datetime'].dt.round('10min')
        df_latlong['Latitude'] = df_latlong['Latitude'].map('{:.6f}'.format)
        df_latlong['Longitude'] = df_latlong['Longitude'].map('{:.6f}'.format)
        df_latlong['Latitude'] = df_latlong['Latitude'].astype(float)
        df_latlong['Longitude'] = df_latlong['Longitude'].astype(float)
        column_grouped_by = ['Datetime']
        df_grouped = df_latlong.groupby(column_grouped_by).agg({
            'Latitude': 'mean', 'Longitude': 'mean'})

        return df_grouped.reset_index()

    def df_all_remotes_rawipstats(self, common_ids, array_file_names, folder_path):
        array_remote_dfs = []
        for remote_id in common_ids:
            array_filtered_files = self.filter_files_by_remote_id(
                remote_id, array_file_names)
            df_rawipstats = self.read_filtered_files_rawipstats(folder_path,
                                                                array_filtered_files)
            df_rawipstats = self.process_df_rawipstats(df_rawipstats)
            df_rawipstats['Remote_id'] = remote_id
            array_remote_dfs.append(df_rawipstats)

        full_df_rawipstats = pd.concat(array_remote_dfs, ignore_index=True)

        return full_df_rawipstats

    def df_all_remotes_latlong(self, common_ids, array_file_names, folder_path):
        array_remote_dfs = []
        for remote_id in common_ids:
            array_filtered_files = self.filter_files_by_remote_id(
                remote_id, array_file_names)
            df_latlong = self.read_filtered_files_latlong(folder_path,
                                                          array_filtered_files)
            df_latlong = self.process_df_latlong(df_latlong)
            df_latlong['Remote_id'] = remote_id
            array_remote_dfs.append(df_latlong)

        full_df_latlong = pd.concat(array_remote_dfs, ignore_index=True)

        return full_df_latlong

    @staticmethod
    def merge_rawipstats_latlong_dfs(full_df_rawipstats,
                                     full_df_latlong):
        columns_merge_on = ['Datetime', 'Remote_id']
        columns_order = ['Remote_id', 'Datetime', 'Total UP',
                         'Total Down', 'Latitude', 'Longitude']
        df_merged = pd.merge(full_df_rawipstats, full_df_latlong,
                             on=columns_merge_on, how='inner')
        df_merged = df_merged.reindex(columns=columns_order)

        return df_merged.reset_index(drop=True)


if __name__ == '__main__':
    main()
