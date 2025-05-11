Everything here represents ~300 hours of work including research, reading, brainstorming, data gathering, data processing, more research, debugging, analysis, and writing.
Code is not fully optimized and fairly amateurish, self-taught largely for research purposes bare with me :/

Code Order:
	1.)Data_Aggregation/temp_percentile_calculations.py
		-calculates monthly percentile thresholds for each county 
		-average_temp_calculations.py can be run for extra analysis/visualizations, but was not used for the paper
		-adjust #cpus at bottom as necessary

	2.)Data_Aggregation/ehd_calculations.py
		-adjust #cpus at bottom as necessary

	3.)Data_Aggregation/mortality_aggregation.ipynb
		- data must be obtained separately from the CDC
		- create "Moratlity_Unnested" in ".../Data/Mortality/" with just the mortality data
		- read Mortality_Documentation for more info regarding tape locations
		- note that for ICD-9 codes, E is not included in the tape and is left blank. documentation can be fairly ambiguous to interpret, but any possible variation in tape locations was accounted for
		- via CDC: "applying national comparability ratios to demographic subgroups (age, sex, race, or location) may not be appropriate, since patterns may differ by group". fixed effects account for code changes.

	4.)Data_Aggregation/population_counts_aggregation.ipynb

	5.)Data_Aggregation/analysis_data_aggregation.ipynb

	6.)Analysis/analysis.rmd
		-watch out for strange symbols writing into csv for ages
	
	7.) Visualizations/visualizations.ipynb

		
