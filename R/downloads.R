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


# ------------------------------------------------------------
# Log helpers (unchanged)
# ------------------------------------------------------------
init_logs <- function(path_log){

  make_empty_log <- function() {
    structure(
      list(
        bulletin      = character(0),
        ind           = character(0),
        values        = character(0),
        url           = character(0),
        download_date = character(0),
        path_file     = character(0),
        in_db         = logical(0)
      ),
      row.names = integer(0),
      class = "data.frame"
    )
  }

  if (!file.exists(path_log)) {
    # Create a brand-new empty log with header
    log <- make_empty_log()
    write.table(log, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
    message(sprintf("Initiated empty log in %s\n", path_log))
    return(log)
  }

  # File exists — handle 0-byte or malformed files safely
  if (file.info(path_log)$size == 0) {
    message(sprintf("Existing log at %s is empty; reinitializing.\n", path_log))
    log <- make_empty_log()
    write.table(log, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
    return(log)
  }

  # Try to read; if malformed, recreate empty
  log <- tryCatch(
    read.csv(path_log, stringsAsFactors = FALSE),
    error = function(e) {
      message(sprintf("Log at %s could not be parsed; reinitializing. (%s)\n", path_log, conditionMessage(e)))
      lg <- make_empty_log()
      write.table(lg, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
      lg
    }
  )

  # Ensure required columns exist (schema check)
  req_cols <- c("bulletin","ind","values","url","download_date","path_file","in_db")
  missing <- setdiff(req_cols, names(log))
  if (length(missing)) {
    message(sprintf("Log missing columns %s; reinitializing.\n", paste(missing, collapse=", ")))
    log <- make_empty_log()
    write.table(log, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
  }

  message(sprintf("Log already exists in %s. Loading\n", path_log))
  return(log)
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
