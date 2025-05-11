import os
import rasterio
import geopandas as gpd
import pandas as pd
import numpy as np
from multiprocessing import Pool
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent

#paths relative to the base directory (adjust as needed)
state_shapefile = (BASE_DIR / ".." / "Data" / "Counties_Boundaries" / "tl_2019_us_county" / "tl_2019_us_county.shp").resolve()
daily_data_folder = (BASE_DIR / ".." / "Data"/ "Temperature").resolve()

percentile_checkpoint = daily_data_folder / "final_85th_percentile_thresholds.csv"
ehd_checkpoint = daily_data_folder / "ehd_results_checkpoint.csv"


#load county shapefile
states = gpd.read_file(state_shapefile)
states = states.to_crs("OGC:CRS83")  #ensure correct projection

#load 85th percentile thresholds into a dictionary for fast lookup
print("loading 85th percentile thresholds:")
percentile_df = pd.read_csv(percentile_checkpoint, dtype={"GEOID": "int", "month": "int", "EHD_threshold": "float32"})
percentile_df['GEOID'] = percentile_df['GEOID'].astype(str).str.zfill(5)
threshold_dict = {(row["GEOID"], row["month"]): row["EHD_threshold"] for _, row in percentile_df.iterrows()}

def process_year(year):
    """processes EHD data for a given year and saves results incrementally"""
    print(f"processing year {year}:")
    all_ehds = []

    for month in [6,7,8]:  
        for day in range(1, 32):  
            try:
                date_str = f"{year}{month:02d}{day:02d}"
                bil_file = daily_data_folder / f"PRISM_tmean_stable_4kmD2_{year}0101_{year}1231_bil" / f"PRISM_tmean_stable_4kmD2_{date_str}_bil.bil"

                #open raster file
                with rasterio.open(bil_file) as dataset:
                    tmean_data = dataset.read(1)
                    affine = dataset.transform
                    crs = dataset.crs

                    #mask NoData values early
                    tmean_data = np.ma.masked_equal(tmean_data, -9999)
                    if np.all(tmean_data.mask):
                        continue  

                    rows, cols = np.where(~tmean_data.mask)
                    x_coords, y_coords = rasterio.transform.xy(affine, rows, cols)
                    temperatures = tmean_data[~tmean_data.mask].data

                    #convert to GeoDataFrame
                    points_gdf = gpd.GeoDataFrame({
                        "temperature": temperatures,
                        "geometry": gpd.points_from_xy(x_coords, y_coords),
                        "year": year,
                        "month": month
                    }, crs=crs)

                    #spatial join with counties
                    points_with_counties = gpd.sjoin(
                        points_gdf, states[["GEOID", "NAME", "geometry"]],
                        how="left", predicate="within"
                    )

                    #compute EHDs directly (avoid merging)
                    county_avg = points_with_counties.groupby(["GEOID", "NAME"]).agg(
                        avg_temperature=("temperature", "mean")).reset_index()
                    
                    county_avg["GEOID"] = county_avg["GEOID"].astype(str).str.zfill(5)
                    county_avg['month'] = month
                    county_avg["year"] = year

                    county_avg["EHD_threshold"] = county_avg.apply(
                        lambda row: threshold_dict.get((row["GEOID"], row["month"]), np.nan), axis=1)

                    county_avg.dropna(subset=["EHD_threshold"], inplace=True)


                    # **EHD condition:**
                    county_avg["is_EHD"] = ((county_avg["avg_temperature"] > county_avg["EHD_threshold"]) |
                        (county_avg["avg_temperature"] >= 30.0)).astype(int)

                    #count EHDs per county and month
                    ehd_counts = county_avg.groupby(["GEOID", "NAME", "month"])["is_EHD"].sum().reset_index()
                    ehd_counts["year"] = year  #include the year column

                    all_ehds.append(ehd_counts)

            except Exception as e:
                print(f"error processing {date_str}: {e}")
                continue  

    #save yearly results to disk
    if all_ehds:
        yearly_df = pd.concat(all_ehds, ignore_index=True)

        #append results to checkpoint file
        if os.path.exists(ehd_checkpoint):
            yearly_df.to_csv(ehd_checkpoint, mode='a', header=False, index=False)
        else:
            yearly_df.to_csv(ehd_checkpoint, index=False)

        #free memory
        del yearly_df
        all_ehds.clear()
        print(f"saved EHD results for year: {year}")

def get_processed_years():
    """reads the checkpoint file and returns a set of years already processed"""
    if os.path.exists(ehd_checkpoint):
        try:
            df = pd.read_csv(ehd_checkpoint, usecols=["year"])
            return set(df["year"].unique())  # return unique years as a set
        except Exception as e:
            print(f"error reading checkpoint file: {e}")
            return set()
    return set()

def compute_ehd_results():
    """compute and save the final EHD results across all years"""
    
    #get processed years from the checkpoint
    processed_years = get_processed_years()

    for year in range(1989, 2020):  
        if year not in processed_years:
            process_year(year)  #only process missing years

    #load full EHD dataset
    print("computing the final EHD summary across all years...")
    all_ehd_df = pd.read_csv(ehd_checkpoint)

    final_ehd_results = all_ehd_df.groupby(["GEOID", "NAME", "month", "year"])["is_EHD"].sum().reset_index()

    #save final results 
    final_output_file = daily_data_folder / "county_summer_EHDs_1989_2019.csv"
    final_ehd_results.to_csv(final_output_file, index=False)
    analysis_output = (BASE_DIR / ".." / "Analysis" / "Analysis_Data" / "county_summer_EHDs_1989_2019.csv").resolve()
    final_ehd_results.to_csv(analysis_output, index=False)

    
    print(f"final EHD results saved successfully at {final_output_file}")

    return final_ehd_results

if __name__ == "__main__":
    num_workers = 16  #adjust as needed
    print("starting multiprocessing execution:")
    with Pool(num_workers) as pool:
        pool.map(process_year, range(1989, 2020))
    
    compute_ehd_results()
    print("processing complete!")