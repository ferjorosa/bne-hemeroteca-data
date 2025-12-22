---
license: cc0-1.0
task_categories:
- image-classification
- text-classification
language:
- es
tags:
- historical
- newspapers
- spain
- cultural-heritage
- hemeroteca
- archive
- bne
size_categories:
- 1K<n<10K
---

# BNE Hemeroteca Publications Dataset

Enriched metadata and cover images for historical Spanish publications from the [Biblioteca Nacional de España (BNE) - Hemeroteca Digital](https://hemerotecadigital.bne.es/).

**Why this dataset?** The BNE provides an [open data CSV](https://datosabiertos.bne.es/catalogo/dataset/hemeroteca-digital-texto-completo) with basic publication information, but it lacks key details such as descriptions, number of issues, and direct links to content. This dataset enriches that data by scraping the full metadata from each publication's detail page.

**Use cases:**
- **Explore the archive** — Quickly browse and filter what's available in the Hemeroteca
- **Topic modeling** — Analyze descriptions and metadata to discover thematic patterns
- **Access original issues** — Use `issues_link` to explore and download scanned issues, then apply OCR to extract full text

**Note**: Generated on December 22nd, 2025.

## Fields

| Field | Description |
|-------|-------------|
| `image` | Cover image (when available) |
| `issn` | International Standard Serial Number |
| `title` | Main title |
| `other_title` | Alternative titles |
| `collection` | Collection name |
| `description` | Detailed description |
| `geographic_scope` | Geographic coverage area |
| `publication_place` | Place of publication |
| `date` | Publication date range |
| `language` | Language(s) |
| `issues_count` | Number of issues available |
| `total_pages` | Total pages |
| `detail_link` | Link to publication detail page |
| `issues_link` | Link to browse issues |

## Usage

```python
from datasets import load_dataset

dataset = load_dataset("ferjorosa/bne-hemeroteca-publications")
sample = dataset["train"][0]
print(sample["title"], sample["description"])
```

## Citation

```bibtex
@dataset{bne_hemeroteca_publications,
  title={BNE Hemeroteca Publications Dataset},
  author={Fernando Rodriguez},
  year={2025},
  url={https://huggingface.co/datasets/ferjorosa/bne-hemeroteca-publications},
  note={Compiled from Hemeroteca Digital (Biblioteca Nacional de España)}
}
```

## License

CC0 1.0 (Public Domain). Original content provided by the [Biblioteca Nacional de España](https://www.bne.es/).
