import os
import rasterio
import geopandas as gpd
import pandas as pd
import numpy as np
from multiprocessing import Pool
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

#paths relative to the base directory (adjust as needed)
state_shapefile = ( BASE_DIR / ".." / "Data" / "Counties_Boundaries" / "tl_2019_us_county" / "tl_2019_us_county.shp").resolve()
daily_data_folder = (BASE_DIR / ".." / "Data"/ "Temperature").resolve()

percentile_checkpoint = daily_data_folder / "percentile_thresholds_checkpoint.csv"

#load county shapefile
states = gpd.read_file(state_shapefile)
states = states.to_crs("OGC:CRS83")  #ensure correct projection

def process_year(year):
    """process temperature data for a given year and save results incrementally"""
    print(f"processing year {year}:")
    all_temps = []

    for month in [6, 7, 8]:  
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
                        "year": year,  #include the year column
                        "month": month
                    }, crs=crs)

                    #spatial join with counties
                    points_with_counties = gpd.sjoin(
                        points_gdf, states[["GEOID", "NAME", "geometry"]],
                        how="left", predicate="within"
                    )

                    all_temps.append(points_with_counties)

            except Exception as e:
                continue  

    #save yearly results to disk
    if all_temps:
        yearly_df = pd.concat(all_temps, ignore_index=True)
        yearly_df = yearly_df[["GEOID", "month", "temperature", "year"]]  

        #append results to checkpoint file
        if os.path.exists(percentile_checkpoint):
            yearly_df.to_csv(percentile_checkpoint, mode='a', header=False, index=False)
        else:
            yearly_df.to_csv(percentile_checkpoint, index=False)

        #free memory
        del yearly_df
        all_temps.clear()
        print(f"saved results for year {year}")

def get_processed_years():
    """reads the checkpoint file and returns a set of years already processed"""
    if os.path.exists(percentile_checkpoint):
        try:
            df = pd.read_csv(percentile_checkpoint, usecols=["year"])  
            return set(df["year"].unique())  
        except Exception as e:
            print(f"error reading checkpoint file: {e}")
            return set()
    return set()

def compute_percentile_threshold():
    """compute the final 85th percentile across all years for each county and summer month"""
    
    #get processed years from the checkpoint
    processed_years = get_processed_years()

    for year in range(1989, 2020):  
        if year not in processed_years:
            process_year(year)  #only process missing years

    #compute overall 85th percentile across all stored years
    print("computing the final 85th percentile across all years:")
    all_temps_df = pd.read_csv(percentile_checkpoint)

    final_percentile_thresholds = all_temps_df.groupby(["GEOID", "month"])["temperature"].quantile(0.85).reset_index()
    final_percentile_thresholds.rename(columns={"temperature": "EHD_threshold"}, inplace=True)

    #save final results
    final_output_file = daily_data_folder / "final_85th_percentile_thresholds.csv"
    final_percentile_thresholds.to_csv(final_output_file, index=False)
    
    print(f"final percentile thresholds saved successfully at {final_output_file}")

    return final_percentile_thresholds

if __name__ == "__main__":
    num_workers = 5  #adjust as needed
    print("starting multiprocessing execution:")
    with Pool(num_workers) as pool:
        pool.map(process_year, range(1989, 2020))
    
    compute_percentile_threshold()
    print("processing complete!")
