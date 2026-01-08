import geopandas, geodatasets

zipfile = "/Users/harsh/Desktop/repos/bigproj/playground/data/ParkServe_fgd_DataShare_05212025.zip!Parkscore_2025_DataDownloads_GDB_05212025/ParkScore_2025_DataDownloads.gdb"
parks = geopandas.read_file(
    zipfile, layer="ParkServe_Parks",
    where="Park_State='New Jersey'"
    # where="Park_State='New Jersey' AND Park_County='Union County'"
)
# playgrounds = geopandas.read_file(
#     zipfile, layer="ParkServe_Playgrounds",
#     # where="Park_State='New Jersey' AND Park_County='Union County'"
# )
print(geopandas.list_layers(zipfile))

m = parks.explore()
# m = playgrounds.explore()

m.save("map.html")
