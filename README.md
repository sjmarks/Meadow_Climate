# Meadow_Climate

Processing functions for climate data collected at Rock Creek and Control meadows near Chester, CA. Supports the research effort investigating the hydrologic response of montane meadows by conifer removal restoration. Research conducted by PI Chris Surfleet, California Polytechnic State University San Luis Obispo, NRES Dept.

The **functions** contained in this repo:

1. Read raw `.csv` files collected from Hobo Onset water depth recorders (U20/U20L) (atmospheric data in this context).
2. Read raw `.csv` files collected from Hobo Onset climate station.
3. Compile atmospheric and climate data for a meadow site in chronological order. Data is compiled in a "tidy" format.
4. Update a previous compilation for a given meadow site with newly collected raw data files from the field.

Functions for processing are contained in `climate_processing_funs.R`. Most recent compilation for RC and Control climate is May 2021, completed in `RC_Control_Climate_May2021.Rmd`.
