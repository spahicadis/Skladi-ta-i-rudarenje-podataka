from transform.dimensions.camera_dim import transform_camera_dim
from transform.dimensions.conditions_dim import transform_conditions_dim
from transform.dimensions.date_dim import transform_date_dim
from transform.dimensions.observation_dim import transform_observation_dim
from transform.dimensions.time_dim import transform_time_dim
from transform.dimensions.vehicle_dim import transform_vehicle_dim
from transform.dimensions.vlm_dim import transform_vlm_dim
from transform.facts.vehicle_detection_fact import transform_vehicle_detection_fact


# Redoslijed nije proizvoljan: sve dimenzije nastaju prije tablice činjenica
# jer ona od njih preuzima surogat ključeve.
def run_transformations(raw_data):
    csv_df = raw_data.get("csv_detections")

    date_dim = transform_date_dim(raw_data["motion"], csv_df)
    print("1 Date dimension complete")

    time_dim = transform_time_dim(raw_data["motion"], csv_df)
    print("2 Time dimension complete")

    camera_dim = transform_camera_dim(raw_data["camera"], raw_data["location"])
    print("3 Camera dimension complete")

    vehicle_dim = transform_vehicle_dim(
        raw_data["vehicle"], raw_data["class"], raw_data["brand"], raw_data["color"], csv_df
    )
    print("4 Vehicle dimension complete")

    conditions_dim = transform_conditions_dim(
        raw_data["weather_observation"], raw_data["motion"], csv_df
    )
    print("5 Conditions dimension complete")

    observation_dim = transform_observation_dim(
        raw_data["vehicle"], raw_data["inference"], raw_data["motion"], csv_df
    )
    print("6 Observation dimension complete")

    vlm_dim = transform_vlm_dim(raw_data["vlm"])
    print("7 VLM dimension complete (SCD Type 2)")

    vehicle_detection_fact = transform_vehicle_detection_fact(
        raw_data, date_dim, time_dim, camera_dim, vehicle_dim,
        conditions_dim, vlm_dim, observation_dim, csv_df,
    )
    print("8 Vehicle detection fact table complete")

    return {
        "dw.dim_date": date_dim,
        "dw.dim_time": time_dim,
        "dw.dim_camera": camera_dim,
        "dw.dim_vehicle": vehicle_dim,
        "dw.dim_conditions": conditions_dim,
        "dw.dim_observation": observation_dim,
        "dw.dim_vlm": vlm_dim,
        "dw.fact_vehicle_detection": vehicle_detection_fact,
    }
