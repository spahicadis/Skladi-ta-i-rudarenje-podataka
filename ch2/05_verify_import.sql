/* Ručna provjera unosa podataka u bazu podataka
Output selecta mora u potpunosti odgovarati csv podacima iz datoteke
*/

SELECT current_database();

SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' ORDER BY table_name;

SELECT m.id || '-' || v.seq   AS inference_id
, m.id                        AS motion_id
, m.ts                        AS ts
, m.file                      AS file
, cam.code                    AS camera_id
, m.cause                     AS cause
, m.peak_mass                 AS peak_mass
, m.dur_ms                    AS dur_ms
, m.pts_ms                    AS pts_ms
, vl.schema_v                 AS schema_v
, wo.temperature              AS temp_c
, wo.precipitation_mm         AS precip_mm
, wo.weather_code             AS weather_code
, m.lighting                  AS lighting
, i.road_condition             AS road_condition
, cl.name                     AS vehicle_class
, co.name                     AS color
, v.body                      AS body
, br.name                     AS make
, v.model                     AS model
, v.era                       AS era
, v.direction                 AS direction
, v.headlights_on             AS headlights_on
, v.commercial_markings       AS commercial_markings
, v.special_use               AS special_use
, v.truncated                 AS truncated
, v.plate_visible             AS plate_visible
FROM vehicle v
JOIN inference i            ON v.inference_fk = i.id
JOIN motion m               ON i.motion_fk = m.id
JOIN vlm vl                 ON i.vlm_fk = vl.id
JOIN camera cam             ON m.camera_fk = cam.id
JOIN weather_observation wo ON m.weather_observation_fk = wo.id
JOIN "class" cl              ON v.class_fk = cl.id
LEFT JOIN brand br          ON v.brand_fk = br.id
LEFT JOIN color co          ON v.color_fk = co.id
ORDER BY v.id ASC
LIMIT 100;
