# Heat, Health, and Harm: The Impact of Extreme Temperatures on Suicide

**Yegor Baranovski**
Honors Economics Undergraduate Thesis, University of Wisconsin--Madison (2025)

[Paper (PDF)](Heat-Health-and-Harm.pdf)

---

## Overview

This paper estimates the **causal effect of extreme heat on suicide mortality** in the United States from 1989 to 2019. Using comprehensive county-level data on all deaths by suicide paired with high-resolution satellite-based temperature measurements, I exploit year-over-year fluctuations in summer extreme heat days (EHDs) within counties to identify the effect of temperature on mental health-related mortality.

### Key Findings

- **One additional extreme heat day** in a summer month increases the monthly suicide rate by **0.091 deaths per million** residents (a **0.39%** increase relative to baseline).
- Effects are **concentrated among men** (0.154 per million) and **working-age adults** aged 25-64 (0.123 per million)-groups already characterized by elevated baseline suicide risk.
- Results are **remarkably consistent across urban and rural counties** (0.089 vs. 0.093 per million).
- Findings are robust to alternative specifications, including Poisson regression models.
- Because the EHD definition captures only the most severe temperature exposures, these estimates represent a possible **lower bound** on the true impact.

## Methodology

**Extreme Heat Days (EHDs)** are defined as days where the daily mean temperature exceeds either:
1. The **85th percentile** of the 30-year historical monthly distribution for that county, or
2. An **absolute threshold of 30°C**

The identification strategy uses a **panel fixed-effects regression** with county-by-month, county-by-year, and year-by-month fixed effects, isolating the causal effect from idiosyncratic within-county temperature fluctuations. Regressions are weighted by 2000 Census county populations with standard errors clustered at the county level.

Mental health-related mortality is defined broadly to include suicides, injuries of undetermined intent, and certain accidental deaths (poisoning, firearm, train, drowning) to address known misclassification in mortality data.

## Data Sources

| Data | Source | Access |
|------|--------|--------|
| Mortality | [NCHS Restricted-Use Vital Statistics](https://www.cdc.gov/nchs/nvss/nvss-restricted-data.htm) | Application required |
| Temperature | [PRISM Climate Group](https://prism.oregonstate.edu/) | Public (4km daily grids) |
| Population | [SEER Program](https://seer.cancer.gov/popdata/) | Public |
| Education | [USDA Economic Research Service](https://www.ers.usda.gov/data-products/county-level-data-sets/) | Public |
| Urbanization | [NCHS Urban-Rural Classification (2013)](https://www.cdc.gov/nchs/data_access/urban_rural.htm) | Public |
| County boundaries | [US Census TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) | Public |

> **Note:** The restricted-use mortality microdata must be obtained separately from the CDC. See `Data/Mortality/READ.ME.txt` and `Mortality_Documentation.xlsx` for details.

## Repository Structure

```
├── Paper/                      # LaTeX source and figures
│   ├── main.tex                #   Thesis manuscript
│   └── bibliography.bib        #   References
├── Data_Aggregation/           # Data processing pipeline (Python/Jupyter)
│   ├── 1_temp_percentile_calculations.py
│   ├── 2_ehd_calculations.py
│   ├── 3_mortality_aggregation.ipynb
│   ├── 4_population_counts_aggregation.ipynb
│   └── 5_analysis_data_aggregation.ipynb
├── Analysis/                   # Statistical analysis (R)
│   └── 6_analysis.Rmd
├── Visualizations/             # Figure generation (Jupyter)
│   └── visualizations.ipynb
├── Data/                       # Raw and intermediate data
│   ├── Temperature/            #   PRISM-derived county-level temperature data
│   ├── Mortality/              #   CDC mortality data (restricted, not included)
│   ├── Population_Data/        #   SEER population counts
│   ├── Additional Data/        #   Education and urbanization classifications
│   └── Counties_Boundaries/    #   TIGER/Line shapefiles
└── Heat-Health-and-Harm.pdf    # Compiled paper
```

## Replication

Scripts are numbered and should be run in order. The pipeline requires Python (with multiprocessing), Jupyter, and R.

| Step | Script | Description |
|------|--------|-------------|
| 1 | `Data_Aggregation/1_temp_percentile_calculations.py` | Compute monthly 85th-percentile temperature thresholds per county |
| 2 | `Data_Aggregation/2_ehd_calculations.py` | Count extreme heat days per county-month-year |
| 3 | `Data_Aggregation/3_mortality_aggregation.ipynb` | Aggregate restricted mortality microdata by county-month-year and subgroup |
| 4 | `Data_Aggregation/4_population_counts_aggregation.ipynb` | Build county-level population denominators from SEER data |
| 5 | `Data_Aggregation/5_analysis_data_aggregation.ipynb` | Merge temperature, mortality, and population into analysis dataset |
| 6 | `Analysis/6_analysis.Rmd` | Run fixed-effects and Poisson regressions |
| 7 | `Visualizations/visualizations.ipynb` | Generate all figures |

> Steps 1 and 2 support multiprocessing -- adjust the number of CPUs at the bottom of each script as needed.
>
> Step 3 requires the restricted CDC mortality data placed in `Data/Mortality/Mortality_Unnested/`. See `Mortality_Documentation.xlsx` for tape layout details.

## Citation

```bibtex
@thesis{baranovski2025heat,
  title     = {Heat, Health, and Harm: The Impact of Extreme Temperatures on Suicide},
  author    = {Baranovski, Yegor},
  year      = {2025},
  school    = {University of Wisconsin--Madison},
  type      = {Honors Undergraduate Thesis}
}
```

## License

This repository contains academic research code and data processing scripts. The restricted-use mortality data is subject to CDC data use agreements and is not included.
