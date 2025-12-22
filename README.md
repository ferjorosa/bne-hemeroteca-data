# BNE Hemeroteca Data

Code repository for generating datasets from the [Hemeroteca Digital (Biblioteca Nacional de España)](https://hemerotecadigital.bne.es/).

## Motivation

The BNE Hemeroteca contains a treasure trove of historical Spanish newspapers, magazines, and periodicals — some dating back centuries. Making this content searchable and accessible opens doors for researchers, historians, journalists, and anyone interested in Spanish cultural heritage.

## Datasets

### Publications

Enriched metadata and cover images for all publications in the Hemeroteca. Includes titles, descriptions, publication dates, geographic scope, issue counts, and direct links to browse individual issues.

📦 [ferjorosa/bne-hemeroteca-publications](https://huggingface.co/datasets/ferjorosa/bne-hemeroteca-publications)

> **Note**: The BNE provides an [open data CSV](https://datosabiertos.bne.es/catalogo/dataset/hemeroteca-digital-texto-completo) with basic publication information, but it lacks key details such as descriptions, number of issues, and direct links to content. This project enriches that data by scraping the full metadata from each publication's detail page.

## Updates

**December 22, 2025** — Repository initialized. Released the publications dataset with ~3,600 publications scraped from the Hemeroteca Digital.

## Setup

This project uses [UV](https://github.com/astral-sh/uv) as the package manager.

1. Install UV if you haven't already:

```shell
curl -sSf https://astral.sh/uv/install.sh | sh
```

2. Create a virtual environment and install dependencies:

```shell
uv sync
```

3. Run the scraper:

```shell
uv run python scrape_publications.py
```

## Ethical Note

This project respects the BNE's servers by implementing rate limiting and polite delays between requests. The digitized content in the Hemeroteca is made available by the BNE for public access and research purposes. This project aims to facilitate access to this cultural heritage, not to overload or abuse the service.