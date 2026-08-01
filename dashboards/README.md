# AI/BI Dashboards — AstraZeneca (synthetic portfolio data)

**Disclaimer:** Synthetic demo data inspired by public AstraZeneca brands. Not official AZ data or affiliation.

## Setup

1. Run **`notebooks/11_seed_lakehouse_data.sql`** on **SQL warehouse** (Run all).
2. Confirm last cells show **~18,000** claims and **10** products.
3. **Start SQL warehouse** → Import dashboards below.

## Dashboards (import these)

| File | Contents |
|------|----------|
| **`astrazeneca_executive.lvdash.json`** | KPIs, fills by brand (Tagrisso, Imfinzi, Farxiga…), franchise revenue |
| **`astrazeneca_franchise.lvdash.json`** | Oncology / Respiratory / CVRM / Rare Disease, product catalog table |
| **`astrazeneca_patient_journey.lvdash.json`** | Adherence by brand, at-risk outreach table |

Legacy Magnolia dashboards (`therapy_executive`, etc.) still work but use old brand codes.

## AZ brands in seed data

| Brand | Franchise | Molecule |
|-------|-----------|----------|
| Tagrisso | Oncology | osimertinib |
| Imfinzi | Oncology | durvalumab |
| Lynparza | Oncology | olaparib |
| Calquence | Oncology | acalabrutinib |
| Farxiga | CVRM | dapagliflozin |
| Brilinta | CVRM | ticagrelor |
| Fasenra | Respiratory | benralizumab |
| Tezspire | Respiratory | tezepelumab |
| Symbicort | Respiratory | budesonide-formoterol |
| Ultomiris | Rare Disease | ravulizumab |

Tables: `main.gold.az_product_dim`, `main.gold.daily_therapy_metrics`, `main.gold.daily_franchise_metrics`, `main.gold.patient_journey`.
