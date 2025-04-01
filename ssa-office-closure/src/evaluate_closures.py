import pandas as pd
import duckdb
from typing import List, Dict
import argparse
from datetime import timedelta
from deap import base, creator, tools, algorithms
import random
import numpy as np
from tqdm import tqdm

# Create DEAP types at module level
creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)

def format_duration(seconds: float) -> str:
    """Format duration in seconds to a human readable string."""
    td = timedelta(seconds=seconds)
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    return f"{hours}h {minutes}m"

def calculate_drive_time_increase(
    conn: duckdb.DuckDBPyConnection,
    offices_to_close: List[str],
    include_tracts: bool = False
) -> Dict:
    """
    Calculate the increase in drive times when closing specific offices.

    Args:
        conn: DuckDB connection with travel data already loaded
        offices_to_close: List of office codes to close

    Returns:
        Dictionary containing statistics about the impact of closures
    """
    # Get the query results
    results_df = conn.execute("""
        WITH
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
                    "office code" NOT IN (SELECT unnest(?))
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
    total, 
    over_65
FROM
    nearest_office N
    JOIN after_close A ON N.destination_id = A.destination_id
    JOIN './data/ssa-populations.csv' AS P ON 
        N.destination_id = P.geoid
    -- JOIN './data/tract/cb_2023_us_tract_5m.shp' AS G ON G.GEOID = N.destination_id
WHERE 
    (? OR A.duration_sec - N.duration_sec > 0)
ORDER BY
    A.duration_sec - N.duration_sec DESC 
    """, [offices_to_close, include_tracts]).df()

    # Calculate statistics
    total_increase = (results_df['increase_sec'] * results_df['total']).sum()
    total_population = results_df['total'].sum()
    avg_increase = total_increase / total_population
    
    return {
        'total_increase': total_increase,
        'total_population': total_population,
        'avg_increase': avg_increase,
        'results_df': results_df
    }

def get_all_offices(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Get list of all unique office codes from the travel data."""
    offices = conn.execute("SELECT DISTINCT \"office code\" FROM travel").fetchall()
    return [office[0] for office in offices]

def create_individual(offices: List[str], target_closures: int) -> creator.Individual:
    """Create a random individual with exactly target_closures offices."""
    ind = creator.Individual(random.sample(offices, target_closures))
    return ind

def evaluate_individual(individual: creator.Individual, conn: duckdb.DuckDBPyConnection) -> tuple:
    """Evaluate the fitness of an individual (set of offices to close)."""
    results = calculate_drive_time_increase(conn, individual)
    return results['avg_increase'],

def main():
    parser = argparse.ArgumentParser(
        description='Find optimal set of SSA offices to close')
    parser.add_argument(
        'ssa_times_path', help='Path to the SSA times parquet file')
    parser.add_argument('--num-closures', type=int, required=True,
                        help='Number of offices to close')
    parser.add_argument('--population-size', type=int, default=50,
                        help='Population size for genetic algorithm')
    parser.add_argument('--generations', type=int, default=100,
                        help='Number of generations to run')
    parser.add_argument('--mutation-rate', type=float, default=0.1,
                        help='Mutation rate for genetic algorithm')
    parser.add_argument('--output', type=str, help='Path to save results parquet file')
    
    args = parser.parse_args()

    # Create a single DuckDB connection and load the data
    conn = duckdb.connect(':memory:')
    conn.execute(f"CREATE TABLE travel AS SELECT * FROM read_parquet('{args.ssa_times_path}')")

    # Get all possible offices
    all_offices = get_all_offices(conn)
    
    # Initialize toolbox
    toolbox = base.Toolbox()
    toolbox.register("individual", create_individual, all_offices, args.num_closures)
    toolbox.register("population", tools.initRepeat, creator.Individual, toolbox.individual)
    toolbox.register("evaluate", evaluate_individual, conn=conn)
    toolbox.register("mate", tools.cxTwoPoint)
    toolbox.register("mutate", tools.mutShuffleIndexes, indpb=args.mutation_rate)
    toolbox.register("select", tools.selNSGA2)
    
    # Create initial population
    pop = toolbox.population(n=args.population_size)
    
    # Create progress bar
    pbar = tqdm(total=args.generations, desc="Optimizing office closures")
    
    # Run the algorithm with progress bar
    for gen in range(args.generations):
        # Evaluate the individuals
        offspring = algorithms.varOr(pop, toolbox, lambda_=args.population_size, cxpb=0.7, mutpb=args.mutation_rate)
        fits = toolbox.map(toolbox.evaluate, offspring)
        for fit, ind in zip(fits, offspring):
            ind.fitness.values = fit
        pop = toolbox.select(pop + offspring, args.population_size)
        pbar.update(1)
    
    pbar.close()
    
    # Get the best individual
    best_ind = tools.selBest(pop, 1)[0]
    
    # Calculate final impact
    results = calculate_drive_time_increase(conn, best_ind, include_tracts=False)
    
   
    
    # Print results
    print("\nOptimal Office Closures:")
    print(f"Offices to close: {', '.join(best_ind)}")
    print(f"\nImpact Summary:")
    print(f"Total households impacted: {results['total_population']:,.0f}")
    print(f"Weighted average drive time increase: {format_duration(results['avg_increase'])}")

    # Save results to parquet file if output path is specified
    if args.output:

        results = calculate_drive_time_increase(conn, best_ind, include_tracts=True)
        
        results_df = results['results_df']
        
  
        
        # Save to parquet file
        results_df.to_parquet(args.output)
        print(f"\nResults saved to: {args.output}")
     
     # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
