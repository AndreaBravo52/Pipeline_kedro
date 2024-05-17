from kedro.pipeline import Pipeline, node, pipeline
from .nodes import to_datetime, crear_conjuntos, duracion_promedio, num_tot_viajes,crear_dataset_final
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=to_datetime,
                inputs="tripdata",
                outputs="preprocessed_tripdata",
                name="to_datetime_node",
            ),
            node(
                func=crear_conjuntos,
                inputs="preprocessed_tripdata",
                outputs=["dataset1","dataset2"],
                name="crear_conjuntos_node",
            ),
            node(
                func=duracion_promedio,
                inputs="dataset1",
                outputs="dataset1_processed",
                name="duracion_promedio_node",
            ),
            node(
                func=num_tot_viajes,
                inputs="dataset2",
                outputs="dataset2_processed",
                name="num_tot_viajes_node",
            ),
            node(
                func=crear_dataset_final,
                inputs=["dataset1_processed","dataset2_processed"],
                outputs="data_summary",
                name="crear_dataset_final_node",
            ),


        ]
    )