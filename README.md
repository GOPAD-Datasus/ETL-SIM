# ETL Project: SIM

A simple ETL (Extract, Transform, Load) pipeline to facilitate the use of
[DataSUS](https://datasus.saude.gov.br/transferencia-de-arquivos/)'s SIM database.
With focus on the files corresponding to the years 2012 to 2023.

## 📌 Overview
- **Extract**: Data sourced from [OpenDataSUS](https://opendatasus.saude.gov.br/), and stored in ``temp/raw`` folder with .csv format.
- **Transform**: Every year goes through year specific changes and dtypes are optimized. At the end, They
are stored inside ``temp/processed``
- **Load**: ⚠ Under construction (will include structural validation and database loading) ⚠.
  - The files created can be accessed at [DB-SIM](https://github.com/GOPAD-Datasus/DB_SIM)

## 🚀 Setup
1. Clone the repo:
   ```bash
   git clone https://github.com/GOPAD-Datasus/ETL-SIM
   ```
2. Install dependencies
   ```bash
   poetry install
   ```

## ⚙ Run
```bash
python __main__.py
```

## 🔮 Future Features
- Standardization of columns
- Load Phase:
  - Structural validation checks 
  - PostgreSQL database integration (optional)
  - Improved error handling 
- Schema: Optimize final table structure
- Docs: In-depth documentation

## ✨ Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

## 📝 License
[LGNU](LICENSE) | © GOPAD 2025