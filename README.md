# BiroCo Data Generator

This project is used to generate mock business data for the BiroCo scenario.
It is intended for database coursework, testing, and demos.

## Main Purpose

- Generate test datasets across multiple business tables (CSV/SQL/SQLite).
- Control data volume and generation rules through configuration.
- Validate generated outputs with basic consistency checks.

## Project Structure

- `src/data_generator.py`: Main script for data generation.
- `src/validation.py`: Validation script for generated data.
- `src/resources/`: Reference files used during generation.
- `src/output/`: Generated output files (ignored in `.gitignore`).

## Quick Start

```bash
python src/data_generator.py
python src/validation.py
```

Generated files will be written to `src/output/`.

## License

This project is licensed under the MIT License. See `LICENSE` for details.
