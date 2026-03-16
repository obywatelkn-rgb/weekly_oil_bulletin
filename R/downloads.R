# Modern Weekly Oil Bulletin downloader (2026+)
# ------------------------------------------------------------
# This file supersedes the legacy PDF-based logic and
# the former ec.europa.eu/energy/observatory URLs.
# It scrapes the official EC "Weekly Oil Bulletin" page and
# downloads the weekly XLSX files directly.
#
# Source of truth (current EC site):
# https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en
# ------------------------------------------------------------

# Dependencies used here:
# - rvest, xml2  : HTML parsing
# - readxl       : reading XLSX (used in make_db)
# - base R       : regex, download.file, data.frame ops

# ---------------------------------------------------------------------
# NEW (2026+): Scrape XLSX links from the EC Weekly Oil Bulletin page
# ---------------------------------------------------------------------
get_wob_xlsx_links <- function() {
  # Official page (continually updated; weekly cadence)
  url_page <- "https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en"

  # Read HTML
  html <- xml2::read_html(url_page)

  # Grab all anchor hrefs
  links <- rvest::html_nodes(html, "a")
  hrefs <- rvest::html_attr(links, "href")
  hrefs <- hrefs[!is.na(hrefs)]

  # Keep only .xlsx files
  xlsx_links <- hrefs[grepl("\\.xlsx$", hrefs, ignore.case = TRUE)]

  # Normalize to absolute URLs
  xlsx_links <- ifelse(
    grepl("^https?://", xlsx_links, ignore.case = TRUE),
    xlsx_links,
    paste0("https://energy.ec.europa.eu", xlsx_links)
  )

  # Build simple data.frame
  df <- data.frame(
    url  = xlsx_links,
    file = basename(xlsx_links),
    stringsAsFactors = FALSE
  )

  # Filter to weekly "prices" files only:
  #  - include 'price' in filename
  #  - exclude 'developments' and 'duties' overview files
  keep <- grepl("price", df$file, ignore.case = TRUE) &
          !grepl("developments|dutie", df$file, ignore.case = TRUE)
  df <- df[keep, , drop = FALSE]

  # Try to extract a YYYYMMDD (or YYYY_MM_DD / YYYY-MM-DD) from filename
  pattern <- "(\\d{4}[ _-]?\\d{2}[ _-]?\\d{2})"
  m <- regexpr(pattern, df$file, perl = TRUE)
  dates_raw <- ifelse(m > 0, regmatches(df$file, m), NA_character_)
  # Normalize date token to YYYY_MM_DD if present
  dates_norm <- ifelse(
    is.na(dates_raw), NA_character_,
    gsub("[_ -]", "_", dates_raw, perl = TRUE)
  )

  # Attach derived date (may be NA if filename has no date token)
  df$date <- dates_norm

  return(df)
}

# ------------------------------------------------------------
# Download newly found WOB XLSX files and update logs
# ------------------------------------------------------------
download_wobs <- function(wobs, logs, path_data = "./data/raw") {

  if (!file.exists(path_data)) {
    dir.create(path_data, recursive = TRUE)
  }

  # Only download URLs not present in log
  dl_mask <- wobs$url %nin% logs$url
  wobs_to_dl <- wobs[dl_mask, , drop = FALSE]

  message(sprintf("Found %d XLSX file(s) for download", nrow(wobs_to_dl)))

  if (nrow(wobs_to_dl) == 0) {
    return(logs)
  }

  for (i in seq_len(nrow(wobs_to_dl))) {

    url <- wobs_to_dl$url[i]
    file_name <- basename(url)
    path_file <- file.path(path_data, file_name)

    message(sprintf("Downloading %s ...", file_name))

    # Best-effort download; if it fails, skip and continue
    dl_ok <- TRUE
    tryCatch({
      utils::download.file(url, destfile = path_file, mode = "wb", quiet = TRUE)
    }, error = function(e) {
      message(sprintf("❌ Failed to download %s (%s)", url, conditionMessage(e)))
      dl_ok <- FALSE
    })

    if (!dl_ok) next

    # Append a log row
    log_row <- data.frame(
      bulletin      = if (!is.null(wobs_to_dl$date[i]) && !is.na(wobs_to_dl$date[i])) wobs_to_dl$date[i] else "",
      ind           = "xlsx",
      values        = if (!is.null(wobs_to_dl$date[i]) && !is.na(wobs_to_dl$date[i])) wobs_to_dl$date[i] else "",
      url           = url,
      download_date = as.character(Sys.Date()),
      path_file     = path_file,
      in_db         = FALSE,
      stringsAsFactors = FALSE
    )

    logs <- rbind(logs, log_row)
  }

  return(logs)
}

# ------------------------------------------------------------
# Database builder: keep your existing logic
# ------------------------------------------------------------
make_db <- function(path_dir_db, logs){

  path_db_bin <- file.path(path_dir_db, "wob_full.rds")
  path_db_csv <- file.path(path_dir_db, "wob_full.csv")

  stopifnot('No entries in logs yet.' = nrow(logs) > 0)

  # grab path file and in db NA/FALSE
  db_mask <- (is.na(logs[['in_db']]) | !logs[['in_db']]) &
             (!is.na(logs[['path_file']]) | nchar(logs[['path_file']]) > 0)

  wobs_to_db <- logs[db_mask, ]

  if (nrow(wobs_to_db) < 1) {
    message("No new data to append.")
    return(logs)
  }

  db <- vector('list', nrow(wobs_to_db))

  # Read with types compatible with weekly prices files
  coltypes <- c(
    "date",
    "text",
    "text",
    "text",
    "text",
    "text",
    "text",
    "text",
    "text"
  )

  for (idx in seq_len(nrow(wobs_to_db))){

    # check if file exists, if not, clear log position
    if (!file.exists(wobs_to_db[idx, 'path_file'])) {
      wobs_to_db[idx, 'path_file'] <- NA
    } else {
      dbfill <- tryCatch({
        readxl::read_excel(
          path      = wobs_to_db[idx, 'path_file'],
          col_types = coltypes,
          col_names = TRUE,
          na        = c("N.A", "N/A")
        )
      }, error = function(e) {
        message(sprintf("❌ Failed to read %s : %s",
                        wobs_to_db[idx, 'path_file'], conditionMessage(e)))
        NULL
      })

      if (is.null(dbfill)) next

      # keep rows with a valid date marker column if present
      if ("Prices in force on" %in% names(dbfill)) {
        db[[idx]] <- dbfill[!is.na(dbfill[["Prices in force on"]]), ]
      } else {
        # fallback: keep all rows (schema may vary slightly)
        db[[idx]] <- dbfill
      }

      # remove commas and coerce numeric in last three cols if present
      if (ncol(db[[idx]]) >= 9) {
        suppressWarnings({
          db[[idx]][, 7:9] <- apply(
            db[[idx]][, 7:9, drop = FALSE],
            MARGIN = 2,
            FUN = function(x) as.numeric(gsub(",", "", x))
          )
        })
      }

      wobs_to_db[idx, 'in_db'] <- TRUE
    }
  }

  # combine and persist
  db <- do.call(rbind, db)
  message(sprintf('Appending %.0f rows to DB', nrow(db)))

  if (!file.exists(path_db_csv)) {
    # write header on first save
    write.table(db, path_db_csv, col.names = TRUE, row.names = FALSE, sep = ";")
  } else {
      # always write CSV with header if schema changed or initial was empty
      write.table(db, path_db_csv, col.names = TRUE, row.names = FALSE, sep = ";")
  }


  if (!file.exists(path_db_bin)) {
    saveRDS(db, path_db_bin)
  } else {
    db_prev <- readRDS(path_db_bin)
    db_all  <- rbind(db_prev, db)
    saveRDS(db_all, path_db_bin)
  }

  logs[db_mask, ] <- wobs_to_db
  logs
}

# ------------------------------------------------------------
# Log helpers (unchanged)
# ------------------------------------------------------------
init_logs <- function(path_log){

  if(!file.exists(path_log)){
    log <- structure(list(bulletin = character(0), ind = character(0), values = character(0),
                          url = character(0), download_date = character(0), path_file = character(0),
                          in_db = logical(0)), row.names = integer(0), class = "data.frame")
    write.table(
      log,
      path_log,
      row.names = FALSE,
      col.names = TRUE,
      sep = ",")
    message(sprintf("Initiated empty log in %s\n", path_log))

  } else {
    message(sprintf("Log already exists in %s. Loading\n", path_log))
    log <- read.csv(path_log)
  }
  return(log)
}

update_logs <- function(logs, path_log){

  if(!file.exists(path_log)){
    write.table(
      logs,
      path_log,
      col.names = TRUE,
      row.names = FALSE,
      sep = ",")
  } else {
    message("Overwriting existing log.")
    write.table(
      logs,
      path_log,
      col.names = TRUE,
      row.names = FALSE,
      sep = ",",
      append = FALSE)
  }
}

# ------------------------------------------------------------
# Misc helpers (some kept for compatibility)
# ------------------------------------------------------------
`%nin%` <- Negate(`%in%`)

make_log_mask <- function(wobs, logs, verbose = FALSE){
  if(nrow(logs) == 0){
    mask <- !logical(nrow(wobs))
  } else {
    mask <- wobs[['url']] %nin% logs[['url']]
  }
  if(verbose){
    message(sprintf("Found %d wob's for download\n", length(mask)))
  }
  return(mask)
}

make_date <- function(x){
  return(strptime(x, "%d/%m/%Y"))
}

# (end of file)
