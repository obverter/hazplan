# Chemical Safety Database

A tool for scraping, organizing, and analyzing chemical safety data from Safety Data Sheets (SDS) and other sources to facilitate the creation of Hazardous Materials Business Plans (HMBP).

## Project Overview

This project aims to create a comprehensive database of chemical safety information by:

1. Scraping data from various SDS sources
2. Organizing and standardizing the data
3. Enabling queries for report generation
4. Supporting HMBP creation for regulatory compliance

## Current Status

This project is in early development. The initial focus is on creating a minimum viable scraper that can extract basic chemical data from targeted websites.

## Directory Structure

- `src/`: Source code for the project
  - `scrapers/`: Web scraping modules
  - `database/`: Database management
  - `utils/`: Utility functions
- `data/`: Data storage
  - `raw/`: Raw scraped data
  - `processed/`: Cleaned and processed data
- `tests/`: Test modules
- `notebooks/`: Jupyter notebooks for exploration and analysis

## Setup and Installation

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the environment:
   - Windows: `venv\Scripts\activate`
   - Unix/MacOS: `source venv/bin/activate`
4. Install requirements: `pip install -r requirements.txt`

## Usage

(Documentation will be added as features are implemented)

## Contributing

This is a personal project currently under development.

## License

[Specify your license here]