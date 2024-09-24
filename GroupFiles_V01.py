# %%
import h5py
import numpy as np
import glob

# %%
def read_hdf_array( hdf5_Input, group, name ):
    return np.array( hdf5_Input[ group + '/' + name ] )

def read_hdf_scalar( hdf5_Input, group, name ):
    return hdf5_Input[ group + '/' + name ][()]

def read_hdf_string( hdf5_Input, group, name ):
    return hdf5_Input[ group + '/' + name ][()]

def save_hdf_array( hdf5_Output, group, name, fdata ):
    hdf5_Output.create_dataset( group + '/' + name, data=fdata, chunks=True, compression='gzip' )

def save_hdf_scalar( hdf5_Output, group, name, fdata ):
    hdf5_Output.create_dataset( group + '/' + name, data=fdata )

def save_hdf_string( hdf5_Output, group, name, fdata ):
    hdf5_Output.create_dataset( group + '/' + name, data=fdata, 
                                dtype=save_hdf_string.dt_str )
save_hdf_string.dt_str = h5py.special_dtype( vlen=bytes )

# %%
def get_dataset_keys(f):
    keys = []
    f.visit(lambda key : keys.append(key) if isinstance(f[key], h5py.Dataset) else None )
    return keys

# %%
def check_regular( file ):
    names = None

    if 'reg' in file:
        kH = file.find( 'H' )+1
        kf = file.find( 'f' )+1
        kS = file.find( 'S' )+1
        
        _H = float( file[kH] ) / 100.0
        _f = float( file[ kf:kf+4 ].replace( '_', '.' ) )
        _S = float( file[ kS:kS+4] ) / 10000.0

        with h5py.File( file, 'a' ) as hf:

            H = read_hdf_scalar( hf, "Data", "H" )
            f = read_hdf_scalar( hf, "Data", "freq" )
            S = read_hdf_scalar( hf, "Data", "S" )

            if np.abs( H -_H ) > 1E-14:
                print( file, "H not match")
            if np.abs( np.round( f, 2 ) -_f ) > 1E-14:
                print( file, "f not match")
            if np.abs( np.round( S, 4 ) -_S ) > 1E-14:
                print( file, "S not match")

            names = f"reg_H{H:.3f}m_S{S*10000:.4f}E-4m2", f"f{f:.3f}Hz"

    return names

# %%
def check_irregular( file ):
    names = None

    if 'irr' in file:

        kH = file.find( 'Hs' )+2
        kT = file.find( 'Tp' )+2
        kS = file.find( 'S' )+1

        _Hs = float( file[kH] ) / 100.0
        _Tp = float( file[ kT:kT+4 ].replace( '_', '.' ) )
        _S = float( file[ kS:kS+4] ) / 10000.0

        with h5py.File( file, 'a' ) as hf:

            Hs = read_hdf_scalar( hf, "Data", "Hs" )
            Tp = read_hdf_scalar( hf, "Data", "Tp" )
            S  = read_hdf_scalar( hf, "Data", "S" )

            if np.abs( Hs -_Hs ) > 1E-14:
                print( file, "Hs not match")
            if np.abs( Tp -_Tp ) > 1E-14:
                print( file, "Tp not match")
            if np.abs( np.round( S, 4 ) -_S ) > 1E-14:
                print( file, "S not match")

            names = f"irr_Hs{Hs:.3f}m_S{S*10000:.4f}E-4m2", f"Tp{Tp:.3f}s"
    return names

# %%
basefilename = '01_*.h5'

file_lst = glob.glob( basefilename )

files_all2one = {}

for file in file_lst:
    if 'reg' in file:
        names = check_regular( file )
    else:
        names = check_irregular( file )
        
    if names is not None:
        files_all2one[ file ] = names

# for key, val in files_all2one.items():
#     print( key, val )


for old_file in file_lst:

    with h5py.File( old_file, 'r' ) as old_hf:
        print( old_file )

        data_lst = get_dataset_keys( old_hf )
        new_file, group = files_all2one[old_file]
    
        with h5py.File( new_file + '.h5', 'a' ) as new_hf:

            for old_data_key in data_lst:
                new_data_key = old_data_key.replace( 'Data', group )

                od = old_hf[ old_data_key ]

                if len( od.shape ) == 0:
                    new_hf.create_dataset( new_data_key, data=od[()], dtype=od.dtype )
                else:
                    new_hf.create_dataset( new_data_key, data=od[()], dtype=od.dtype )#compression="gzip", chunks=True, maxshape=(None,) )
                pass
        pass
