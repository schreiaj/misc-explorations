## Purpose

This was an exploration of the impact the proposed SSA offices would have on folks who rely on them. Specifically I wanted to see how much extra time would be added to the process of going into the office to get an issue handled. This was increasingly important to me given the recent changes to require either online or in person appointments for handling certain issues removing the option of doing it over the phone resulting in an estimated increase 75k in person visits to SSA offices per week. 

## Data Sources

All data was collected from publicly available sources.The data has been replicated in the `data` folder in this repository to facilitate reproduction of this work. However, if you are looking to do similar analysis I recommend pulling the data directly from the sources for the most updated information.

The SSA publishes a list of field offices available on [their website](https://www.ssa.gov/data/maps/accessible.html). I used both the Field Office and Resident Station lists. 

HUD Zip/Tract Crosswalk data is available on the [HUD website](https://www.huduser.gov/apps/public/uspscrosswalk/home) it may require creating an account, once that is done the [direct link](https://www.huduser.gov/apps/public/uspscrosswalk/download_file/ZIP_TRACT_122024.xlsx) should get the correct file.

Routing times were pulled from [OpenTimes](https://opentimes.org/) 

The list of SSA office closures was extracted from the [AP News article](https://apnews.com/article/social-security-offices-closures-doge-trump-b2b1a5b2ba4fb968abc3379bf90715ff) on the topic and matched by hand by state and city. It seems incomplete and that many offices didn't match up to field offices. Instructions for changing what offices are closing will be provided so the data can be updated as more information becomes available. 

Census tract age information was pulled from US Census data, specifically the [B11007 table](https://data.census.gov/table?q=B11007).

## Process

1) Initialize a duckdb database to load the data into. You can do this in memory or on disk. In this example we're going to use an on disk database.

`duckdb ssa-office-closure.db`

2) Field Office and Resident Station was merged into a single csv file (manually concatenated) and imported into duckdb. This file is available in the `data` folder as `ssa-offices.csv` 

`CREATE TABLE ssa_locations AS FROM './data/ssa-offices.csv';`

3) Load the HUD zip/tract crosswalk data into duckdb.

`CREATE TABLE zip_id AS FROM './data/zip_to_tract.csv';`

4) Configure opentimes as a pointer db to use for routing times. More documentation about this can be found at the OpenTimes [docs](https://github.com/dfsnow/opentimes?tab=readme-ov-file#using-duckdb)

```
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://data.opentimes.org/databases/0.0.1.duckdb' AS opentimes;
```

5) Compute the routing times for each census tract to each office. This is the one step that takes reasonable compute time. When I ran it it used about 30gb of memory and took a few minutes to run. The output file will be available at `data/ssa-times.parquet` so you can skip this step if you'd prefer to just load the data from there. I would recommend skipping generating the file and just loading it unless you've changed where the offices are located or have some other reason to regenerate the file.

```
COPY (
 WITH offices AS (
     SELECT "office code",
            (SELECT tract from zip_id where zip = "zip code" LIMIT 1) as tract,
      FROM ssa_locations
 )
 SELECT "office code", origin_id, destination_id, duration_sec
 FROM opentimes.public.times
 JOIN offices ON origin_id = tract
 WHERE
   version = '0.0.1'
   AND mode = 'car'
   AND year = '2024'
   AND geography = 'tract'
 ) TO './data/ssa_times.parquet' (FORMAT PARQUET);
 ```

You can load the data from the generated parquet file using the following command:

 `create table travel as from './data/ssa_times.parquet';`

 6) Load the census tract age data into duckdb. 

 `create table age_demographics as from read_csv('./data/ACSDT5Y2023.B11007-Data.csv', header=true, skip=1);`

 7) The data in that file has some really bad column names but it's easier to just import and then generate a table from what we need because we have to mutate the GEOIDs a bit anyway. 

 `copy(select right(Geography,11) geoid, "Estimate!!Total:!!Households with one or more people 65 years and over:" over_65, "Estimate!!Total:" total  from age_demographics  order by over_65 desc
‣ ) TO './data/ssa-populations.csv';`

8) Compute nearest SSA offices to each census tract and join in the shapefile. Surprisingly this is quite fast.  Note, this only generates data for tracts that change which office is closest. There is a comment in the code that will tell you how to generate data for all tracts.

```
COPY (WITH
    nearest_office AS (
        SELECT
            t.destination_id,
            t.office_code,
            t.duration_sec
        FROM
            (
                SELECT
                    destination_id,
                    "office code" AS office_code,
                    duration_sec,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            destination_id
                        ORDER BY
                            duration_sec ASC
                    ) AS row_num
                FROM
                    travel
            ) t
        WHERE
            t.row_num = 1
    ),
    after_close AS (
        SELECT
            t.destination_id,
            t.office_code,
            t.duration_sec
        FROM
            (
                SELECT
                    destination_id,
                    "office code" AS office_code,
                    duration_sec,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            destination_id
                        ORDER BY
                            duration_sec ASC
                    ) AS row_num
                FROM
                    travel
                    
                WHERE
                    -- If you set this to the offices being closed you'll get the change in times for those who used those offices
                    "office code" NOT IN ('626','765','884','673','605','446','807','645','B14','643','871','482','336','335','340','706','133','148','393','790','839','314','879')

            ) t
        WHERE
            t.row_num = 1
    )
SELECT
    N.destination_id,
    A.duration_sec - N.duration_sec as increase_sec,
    N.duration_sec as now_duration_sec,
    A.duration_sec as after_close_duration_sec,
    N.office_code as nearest_office,
    A.office_code as after_close_office,
    geom
FROM
    nearest_office N
    JOIN after_close A ON N.destination_id = A.destination_id
    JOIN './data/tract/cb_2023_us_tract_5m.shp' ON GEOID = N.destination_id
-- Remove this WHERE clause to get all tracts, the file will be fairly large though
WHERE A.duration_sec - N.duration_sec > 0 
ORDER BY
    A.duration_sec - N.duration_sec DESC ) TO './data/ssa_times.geojson'  WITH (FORMAT GDAL, DRIVER 'GeoJSON');
```

With that you'll have the geojson file (`./data/ssa_times.geojson`) and the population data (`./data/ssa-populations.csv`) used to generate the Tableau visualization. 