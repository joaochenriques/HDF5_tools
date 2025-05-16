import numpy as np
import h5py


def load_data( filename, group ):

    data_dic = {}
    with h5py.File( filename, 'r') as f:
        for ( key, val ) in f[group].items():
            val = f[ group ][ key ][()]
            if type( val ) == np.bytes_:
                val = val.decode('utf-8')
            data_dic[key] = val 
        return data_dic
    

def save_data( filename, grp_name, data_dic ):

    with h5py.File( filename, 'w') as f:
        for (key,val) in data_dic.items():
            if type( val ) == str:
                val = np.bytes_( val.encode() )
            f.create_dataset( grp_name + '/' + key, data=np.array( val ) )


sv_dic = {  'name': 'joão',
            'degree': 3,
            'delta': 0.1,
            'x': np.array( (1,2,3,4,5) ),
            'y': np.array( (10,20,30,40,50) ),
        }

save_data( 'text.h5', 'case', sv_dic )

ld_dic = load_data( 'text.h5', 'case'  )

print( ld_dic )