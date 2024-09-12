import pandas as pd
import numpy as np

def transform(data):
    data['duracion'] = pd.to_numeric(data['duracion'], errors='coerce')
    data['duracion'] = round(data['duracion'] / 10000, 2)
    
    data['popularidad'] = pd.to_numeric(data['popularidad'], errors='coerce')
    data['popularidad'] = round(data['popularidad'] / 10, 2)

    data['popularidad_cadena'] = np.where(data['popularidad'] > 9, 'popular',
                                np.where(data['popularidad'] > 6, 'media popular',
                                'no es tan popular'))
    return data
