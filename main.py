import pandas as pd

# Define file paths
file_jumet = "data/Planning_Jumet_06102025.xlsx"
file_geel = "data/Planning_Geel_06102025.xlsx"
file_triton = "data/Planning_Triton_06102025.xlsx"
export_csv = "data/Planning.csv"

# Read all Excel files
df_jumet = pd.read_excel(file_jumet)
df_geel = pd.read_excel(file_geel)
df_triton = pd.read_excel(file_triton)

# Add a column to identify the source of each dataframe
df_jumet['location'] = 'Jumet'
df_geel['location'] = 'Geel'
df_triton['location'] = 'Triton'

# Combine all dataframes
df_combined = pd.concat([df_jumet, df_geel, df_triton], ignore_index=True)

# Filter out DEPOT locations for stop count only
non_depot_locations = df_combined[df_combined['locationFunction'] != 'DEPOT']

# Count unique non-DEPOT locations per route
stops_count = non_depot_locations.groupby(['location', 'routeId', 'vehicleDriverName'])['locationName'].nunique().reset_index()
stops_count = stops_count.rename(columns={'locationName': 'stops'})

# Get all other aggregations (including DEPOT locations)
df_routes = df_combined.groupby(['location', 'routeId', 'vehicleDriverName']).agg({
    'vehicleLicensePlate': 'first',
    'vehicleLoadingMeters': 'first',
    'distanceToNextInKilometres': lambda x: (x.sum()).astype(int),
    'fillRate': lambda x: (x.max() * 100).round(1),
    'arrivalTime': 'min',
    'departureTime': 'max',
    'vehicleCostPerHour': 'first',
    'vehicleCostPerKm': 'first'
}).reset_index()

# Merge the stop counts (without DEPOT) with the other aggregations
df_routes = df_routes.merge(stops_count, on=['location', 'routeId', 'vehicleDriverName'])

# Remove decimals from routeId - convert to integer
df_routes['routeId'] = df_routes['routeId'].astype(int)

# Calculate time difference first (using original timedelta objects)
df_routes['time_diff_td'] = df_routes['departureTime'] - df_routes['arrivalTime']

# Convert time difference to hours (decimal) for cost calculation
df_routes['time_diff_hours'] = df_routes['time_diff_td'].dt.total_seconds() / 3600

# Calculate costs
df_routes['kosten'] = (df_routes['vehicleCostPerHour'] * df_routes['time_diff_hours'] + 
                       df_routes['vehicleCostPerKm'] * df_routes['distanceToNextInKilometres'])

# Format the kosten column to 2 decimal places
df_routes['kosten'] = df_routes['kosten'].round(2)

# Format time columns for display
for col in ['arrivalTime', 'departureTime']:
    df_routes[col] = (df_routes[col].dt.total_seconds() / 3600).apply(
        lambda x: f"{int(x):02d}:{int((x % 1) * 60):02d}"
    )

# Format time_diff for display
df_routes['time_diff'] = df_routes['time_diff_hours'].apply(
    lambda x: f"{int(x):02d}:{int((x % 1) * 60):02d}"
)

# Rename columns to Dutch
df_routes = df_routes.rename(columns={
    'vehicleLicensePlate': 'voertuig',
    'vehicleLoadingMeters': 'capaciteit', 
    'vehicleDriverName': 'chauffeur',
    'distanceToNextInKilometres': 'afstand_km',
    'fillRate': 'belading',
    'arrivalTime': 'aankomst',
    'departureTime': 'vertrek',
    'time_diff': 'tijdsduur'
})

# Select and reorder the final columns
final_columns = ['location', 'routeId', 'voertuig', 'chauffeur', 'capaciteit', 'stops', 
                 'afstand_km', 'belading', 'aankomst', 'vertrek', 'tijdsduur', 'kosten']

df_routes = df_routes[final_columns]

print(df_routes)
df_routes.to_csv(export_csv)