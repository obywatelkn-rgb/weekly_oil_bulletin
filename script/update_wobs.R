## process WOBs (modern EC-compatible version, 2026+)

source("./R/downloads.R")

path_log    <- "./doc/log/log.csv"
path_dir_db <- "./data/db/"

message("------------------------------------------------------------")
message("Fetching Weekly Oil Bulletin XLSX links from the EC website")
message("------------------------------------------------------------")

# 1) Scrape XLSX links directly from official EC page
#    https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en  [1](https://energy.ec.europa.eu/index_en)
wobs <- get_wob_xlsx_links()

# keep only valid URLs
wobs <- wobs[!is.na(wobs$url), , drop = FALSE]

message(sprintf("Found %d XLSX bulletin files.", nrow(wobs)))

# 2) Initialize or load existing logs
logs <- init_logs(path_log)

# 3) Download all missing XLSX files
logs <- download_wobs(
  wobs      = wobs,
  logs      = logs,
  path_data = "./data/raw"
)

# 4) Build/update the database
logs <- make_db(path_dir_db, logs)

# 5) Save updated logs
update_logs(logs = logs, path_log = path_log)

message("------------------------------------------------------------")
message("Weekly Oil Bulletin update complete.")
message("------------------------------------------------------------")
