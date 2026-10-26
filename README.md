# 📊 Skladišta i rudarenje podataka - Projekt

Fakultet informatike u Puli

Skladište podataka za analizu cestovnog prometa nad vlastitim skupom od 36.287 detekcija vozila: relacijski i dimenzijski model (star schema, 7 dimenzija, SCD tip 2) u PostgreSQL-u, ETL u Apache Sparku iz dva izvora te OLAP analiza u Tableau.

## OLAP analiza (Tableau)

### Ploča 1 — Pregled prometa
Operacije: roll-up, drill-down, slice

![Pregled prometa](https://github.com/user-attachments/assets/109295bf-cdb6-4154-85c9-6554f55e479c)

### Ploča 2 — Ritam prometa i uvjeti
Operacije: dice, pivot

![Ritam prometa i uvjeti](https://github.com/user-attachments/assets/694c6369-a557-4af8-9a4a-22c228a5ad53)

### Ploča 3 — Anomalije i kvaliteta podataka
Operacije: slice, pivot, drill-down

![Anomalije i kvaliteta podataka](https://github.com/user-attachments/assets/53a2c177-763b-4847-90d9-9f99c0eca4d1)
