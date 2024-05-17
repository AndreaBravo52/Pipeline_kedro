import pandas as pd


TEN_MINUTES_IN_SECONDS=600


def to_datetime(tripdata : pd.DataFrame) -> pd.DataFrame:
    #preprocessing de la clase
    tripdata = tripdata.dropna()
    tripdata = tripdata.drop_duplicates()
    tripdata['started_at'] = pd.to_datetime(tripdata['started_at'])
    tripdata['ended_at'] = pd.to_datetime(tripdata['ended_at'])
    tripdata['duration'] = tripdata['ended_at'] - tripdata['started_at']
    tripdata['duration_seconds'] = tripdata['duration'].dt.total_seconds()
    tripdata['start_station_id'] = tripdata['start_station_id'].astype(str)
    tripdata['end_station_id'] = tripdata['end_station_id'].astype(str)
    return tripdata

def crear_conjuntos(tripdata : pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    dataset1 = tripdata[tripdata['duration_seconds'] <= TEN_MINUTES_IN_SECONDS]
    dataset2 = tripdata[tripdata['duration_seconds'] > TEN_MINUTES_IN_SECONDS]
    return dataset1, dataset2

def duracion_promedio(dataset1 : pd.DataFrame) -> pd.DataFrame:
    dataset1_processed = dataset1.groupby('start_station_id')['duration_seconds'].mean().reset_index()
    return dataset1_processed

def num_tot_viajes(dataset2 : pd.DataFrame) -> pd.DataFrame:
    dataset2_processed = dataset2.groupby('start_station_id')['ride_id'].count().reset_index()
    return dataset2_processed

def crear_dataset_final(dataset1_processed : pd.DataFrame, dataset2_processed) -> pd.DataFrame:
    data_summary= pd.DataFrame({
        'Dataset': ['dataset1_processed', 'dataset2_processed'],
        'rows' : [len(dataset1_processed), len(dataset2_processed)]
    })
    return data_summary
