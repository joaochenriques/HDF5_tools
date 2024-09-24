# %%
import h5py
import numpy as np
import glob

# %%
def read_hdf_array( hdf5_Input, group, name ):
    return np.array( hdf5_Input[ group + '/' + name ] )

def save_hdf_scalar( hdf5_Output, group, name, fdata ):
    hdf5_Output.create_dataset( group + '/' + name, data=fdata )


# %%
correct = { 'C1E': ( 2484.2*0.5, 2.52 ), 
            'C2B': ( 2484.2*0.5, 2.52 ), 
            'C3E': ( 2484.2*0.5, 2.52 ), 
            'C1D': ( 248.42*0.5, 2.52 ), 
            'C3D': ( 248.42*0.5, 2.52 ) }

basefilename = '01_*.h5'
check_attr = '/Pressure_Corrected'

file_lst = glob.glob( basefilename )

for file in file_lst:
    print( file )

    with h5py.File( file, 'a' ) as hf:

        cases_names = list( hf.keys() )

        for case_name in cases_names:
    
            var_entry = case_name + '/' + check_attr
            if var_entry not in hf:
                
                save_hdf_scalar( hf, case_name, check_attr, 1 )

                for key, vals in correct.items():
                    var_name = f'Press{key}_VOLTS'
                    var_entry = case_name + '/' + var_name
                    if var_entry in hf:
                        VOLTS = read_hdf_array( hf, case_name, var_name ) 
                        Pascal = vals[0] * ( VOLTS - vals[1] )
                        hf[ case_name + f'/Press{key}'][:] = Pascal


