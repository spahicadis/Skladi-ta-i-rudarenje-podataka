-- Provjera ispravnosti punjenja: jesu li sve tablice napunjene, je li
-- ijedan redak izgubljen između izvora i tablice činjenica, je li ijedan
-- strani ključ ostao prazan i je li zabilježena povijest verzija modela.
SELECT 'dim_date' AS tablica, COUNT(*) AS broj_redaka FROM dw.dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM dw.dim_time
UNION ALL SELECT 'dim_camera', COUNT(*) FROM dw.dim_camera
UNION ALL SELECT 'dim_vehicle', COUNT(*) FROM dw.dim_vehicle
UNION ALL SELECT 'dim_conditions', COUNT(*) FROM dw.dim_conditions
UNION ALL SELECT 'dim_observation', COUNT(*) FROM dw.dim_observation
UNION ALL SELECT 'dim_vlm', COUNT(*) FROM dw.dim_vlm
UNION ALL SELECT 'fact_vehicle_detection', COUNT(*) FROM dw.fact_vehicle_detection;


SELECT
    (SELECT COUNT(*) FROM public.vehicle)            AS izvor_baza,
    7257                                             AS izvor_csv,
    (SELECT COUNT(*) FROM dw.fact_vehicle_detection) AS fact_tablica;


SELECT
    COUNT(*) FILTER (WHERE date_tk        IS NULL) AS null_date,
    COUNT(*) FILTER (WHERE time_tk        IS NULL) AS null_time,
    COUNT(*) FILTER (WHERE camera_tk      IS NULL) AS null_camera,
    COUNT(*) FILTER (WHERE vehicle_tk     IS NULL) AS null_vehicle,
    COUNT(*) FILTER (WHERE conditions_tk  IS NULL) AS null_conditions,
    COUNT(*) FILTER (WHERE vlm_tk         IS NULL) AS null_vlm,
    COUNT(*) FILTER (WHERE observation_tk IS NULL) AS null_observation
FROM dw.fact_vehicle_detection;


SELECT vlm_tk, version, date_from, date_to, vlm_id, model, schema_v, prompt_v
FROM dw.dim_vlm
ORDER BY vlm_tk;


-- Analiza nad napunjenim skladištem: upiti koji pokazuju da model odgovara
-- na pitanja zbog kojih je i građen.
SELECT
    d.full_date,
    d.day_of_week,
    d.is_weekend,
    COUNT(*) AS broj_vozila
FROM dw.fact_vehicle_detection f
JOIN dw.dim_date d ON f.date_tk = d.date_tk
GROUP BY d.full_date, d.day_of_week, d.is_weekend
ORDER BY d.full_date;


SELECT
    v.vehicle_class,
    COUNT(*) AS broj_vozila,
    ROUND(AVG(f.dur_ms)) AS prosjecno_trajanje_ms
FROM dw.fact_vehicle_detection f
JOIN dw.dim_vehicle v ON f.vehicle_tk = v.vehicle_tk
GROUP BY v.vehicle_class
ORDER BY broj_vozila DESC;


SELECT
    t.part_of_day,
    c.weather_category,
    c.lighting,
    COUNT(*) AS broj_vozila
FROM dw.fact_vehicle_detection f
JOIN dw.dim_time t ON f.time_tk = t.time_tk
JOIN dw.dim_conditions c ON f.conditions_tk = c.conditions_tk
GROUP BY t.part_of_day, c.weather_category, c.lighting
ORDER BY broj_vozila DESC
LIMIT 10;


SELECT
    o.special_use,
    v.brand,
    COUNT(*) AS broj_vozila
FROM dw.fact_vehicle_detection f
JOIN dw.dim_observation o ON f.observation_tk = o.observation_tk
JOIN dw.dim_vehicle v ON f.vehicle_tk = v.vehicle_tk
WHERE o.special_use <> 'none'
GROUP BY o.special_use, v.brand
ORDER BY broj_vozila DESC
LIMIT 10;
