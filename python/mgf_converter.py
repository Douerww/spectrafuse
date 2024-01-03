import os
import pyarrow.parquet as pq
import sys


# pxd_folder = 'H:/test/PXD014414'
pxd_folder = sys.argv[1]
os.chdir(pxd_folder)
basename = os.path.basename(pxd_folder)

for folder in os.listdir():
    if folder.startswith('parquet'):
        for parquet_file in os.listdir(folder):
            parquet_file_path = os.path.join(folder, parquet_file)
            print(parquet_file_path)
            parquet_file_obj = pq.ParquetFile(parquet_file_path)

            # for row_group
            for i in range(parquet_file_obj.num_row_groups):
                row_group = parquet_file_obj.read_row_group(i)
                row_group = row_group.to_pandas()
                # for one row
                for idx, row in row_group.iterrows():
                    reference = row['reference_file_name']
                    scan_number = str(row['scan_number'])
                    peptidoform = row['peptidoform']
                    q_value = str(row['protein_global_qvalue'])
                    pep_mass = str(row['exp_mass_to_charge'])
                    charge = str(row['charge'])
                    mz_array = row['mz_array']
                    intensity_array = row['intensity_array']

                    with open(f'mgf_files/{reference}.mgf', 'a') as f:
                        f.write('BEGIN IONS\n')
                        f.write(f'TITLE=id=mzspec:{basename}:'
                                f'{reference}:scan:{scan_number},'
                                f'sequence:{peptidoform},'
                                f'q_value:{q_value}\n')
                        f.write(f'PEPMASS={pep_mass}\n')
                        f.write(f'CHARGE={charge}+\n')
                        for mz, intensity in zip(mz_array, intensity_array):
                            f.write(f'{str(mz)} {str(intensity)}\n')
                        f.write('END IONS\n')
