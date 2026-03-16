# =====================================================================
#  Modern Weekly Oil Bulletin Downloader (2026+)
#  EU Weekly Oil Bulletin scraper + dynamic XLSX parser + DB builder
#  Source: https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en
# =====================================================================

library(xml2)
library(rvest)
library(readxl)

# =====================================================================
# PART 1 — SCRAPER: Get XLSX links from official EC Weekly Oil Bulletin
# =====================================================================

get_wob_xlsx_links <- function() {

  url_page <- "https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en"

  html <- xml2::read_html(url_page)

  hrefs <- html %>%
    rvest::html_nodes("a") %>%
    rvest::html_attr("href") %>%
    Filter(Negate(is.na), .)

  # keep only xlsx
  xlsx_links <- hrefs[grepl("\\.xlsx$", hrefs, ignore.case = TRUE)]

  # ensure absolute URLs
  xlsx_links <- ifelse(
    grepl("^https?://", xlsx_links),
    xlsx_links,
    paste0("https://energy.ec.europa.eu", xlsx_links)
  )

  df <- data.frame(
    url  = xlsx_links,
    file = basename(xlsx_links),
    stringsAsFactors = FALSE
  )

  # Keep only "prices" files; exclude "duties" / "developments"
  keep <- grepl("price", df$file, ignore.case = TRUE) &
          !grepl("develop|dut", df$file, ignore.case = TRUE)

  df <- df[keep, , drop = FALSE]

  # extract date tokens if present (YYYY-MM-DD or DD-MM-YYYY or underscore variants)
  pattern <- "(\\d{4}[-_]\\d{2}[-_]\\d{2}|\\d{2}[-_]\\d{2}[-_]\\d{4})"
  m <- regexpr(pattern, df$file, perl = TRUE)
  dates_raw <- ifelse(m > 0, regmatches(df$file, m), NA_character_)
  dates_norm <- ifelse(
    is.na(dates_raw), NA_character_,
    gsub("[_]", "-", dates_raw)
  )

  df$date <- dates_norm
  df
}



# =====================================================================
# PART 2 — LOG HANDLING (robust and fault‑tolerant)
# =====================================================================

init_logs <- function(path_log) {

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
    log <- make_empty_log()
    write.table(log, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
    message("Initialized new log file.")
    return(log)
  }

  # Handle empty/malformed CSV safely
  if (file.info(path_log)$size == 0) {
    message("Log exists but is empty; reinitializing.")
    log <- make_empty_log()
    write.table(log, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
    return(log)
  }

  log <- tryCatch(
    read.csv(path_log, stringsAsFactors = FALSE),
    error = function(e) {
      message("Log unreadable; recreating: ", conditionMessage(e))
      lg <- make_empty_log()
      write.table(lg, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
      lg
    }
  )

  # Check required columns
  req <- c("bulletin","ind","values","url","download_date","path_file","in_db")
  if (!all(req %in% names(log))) {
    message("Log missing columns — resetting.")
    log <- make_empty_log()
    write.table(log, path_log, row.names=FALSE, col.names=TRUE, sep=",")
  }

  log
}


update_logs <- function(logs, path_log) {
  write.table(
    logs,
    path_log,
    col.names = TRUE,
    row.names = FALSE,
    sep = ",",
    append = FALSE
  )
}



# =====================================================================
# PART 3 — DOWNLOAD NEW XLSX FILES
# =====================================================================

`%nin%` <- Negate(`%in%`)

download_wobs <- function(wobs, logs, path_data = "./data/raw") {

  if (!dir.exists(path_data)) dir.create(path_data, recursive = TRUE)

  dl_mask <- wobs$url %nin% logs$url
  wobs_to_download <- wobs[dl_mask, , drop = FALSE]

  message(sprintf("Found %d XLSX file(s) for download", nrow(wobs_to_download)))

  if (nrow(wobs_to_download) == 0) return(logs)

  for (i in seq_len(nrow(wobs_to_download))) {

    url  <- wobs_to_download$url[i]
    file <- basename(url)
    dest <- file.path(path_data, file)

    message("Downloading ", file)

    ok <- TRUE
    tryCatch(
      utils::download.file(url, destfile = dest, mode = "wb", quiet = TRUE),
      error = function(e) {
        message("❌ Download failed: ", conditionMessage(e))
        ok <<- FALSE
      }
    )

    if (!ok) next

    logs <- rbind(
      logs,
      data.frame(
        bulletin      = ifelse(is.na(wobs_to_download$date[i]), "", wobs_to_download$date[i]),
        ind           = "xlsx",
        values        = ifelse(is.na(wobs_to_download$date[i]), "", wobs_to_download$date[i]),
        url           = url,
        download_date = as.character(Sys.Date()),
        path_file     = dest,
        in_db         = FALSE,
        stringsAsFactors = FALSE
      )
    )
  }

  logs
}



# =====================================================================
# PART 4 — DB BUILDER (dynamically handles ALL modern EC XLSX formats)
# =====================================================================

make_db <- function(path_dir_db, logs) {

  path_db_rds <- file.path(path_dir_db, "wob_full.rds")
  path_db_csv <- file.path(path_dir_db, "wob_full.csv")

  if (!dir.exists(path_dir_db)) dir.create(path_dir_db, recursive = TRUE)

  # rows needing ingestion
  mask <- (is.na(logs$in_db) | !logs$in_db) &
          (!is.na(logs$path_file) & nzchar(logs$path_file))

  wobs_to_db <- logs[mask, , drop = FALSE]

  if (nrow(wobs_to_db) == 0) {
    message("No new rows to ingest.")
    if (file.exists(path_db_rds)) {
      full <- readRDS(path_db_rds)
      write.table(full, path_db_csv, sep=";", row.names=FALSE, col.names=TRUE)
    }
    return(logs)
  }

  # helpers ---------------------------------------------------

  parse_date_from_string <- function(x) {
    x <- gsub("%20", "_", x, fixed = TRUE)
    pat <- "(\\d{4}[-_]\\d{2}[-_]\\d{2}|\\d{2}[-_]\\d{2}[-_]\\d{4})"
    m <- regexpr(pat, x)
    if (m[1] < 0) return(NA_Date_)
    token <- regmatches(x, m)
    p <- unlist(strsplit(token, "[-_]"))
    if (nchar(p[1]) == 4) as.Date(paste(p, collapse="-"))
    else as.Date(paste(p[c(3,2,1)], collapse="-"))
  }

  detect_date <- function(df, fallback) {
    cn <- tolower(gsub("[^a-z0-9]+", " ", names(df)))
    idx <- which(cn %in% c("prices in force on","date","date of data","data","valid on"))
    if (length(idx)==0) return(rep(fallback, nrow(df)))
    v <- df[[idx[1]]]
    suppressWarnings(
      out <- as.Date(v, tryFormats=c("%Y-%m-%d","%d-%m-%Y","%d/%m/%Y","%Y/%m/%d"))
    )
    out[is.na(out)] <- fallback
    out
  }

  detect_country <- function(df) {
    cn <- tolower(gsub("[^a-z0-9]+", " ", names(df)))
    idx <- grep("\\bcountry\\b|member state|\\bgeo\\b|^ms$", cn)
    if (length(idx)) idx[1] else NA_integer_
  }

  cast_numeric <- function(x) {
    if (is.numeric(x)) return(x)
    suppressWarnings(as.numeric(gsub(",", ".", gsub("\\s","", as.character(x)))))
  }

  # parse one file → long DF
  parse_one <- function(fpath) {

    if (!file.exists(fpath)) return(NULL)

    df <- tryCatch(
      readxl::read_excel(fpath, col_names=TRUE, na=c("N.A","N/A")),
      error=function(e) { 
        message("❌ XLSX read failed: ", basename(fpath), " — ", conditionMessage(e))
        return(NULL)
      }
    )

    if (is.null(df) || nrow(df)==0) return(NULL)

    names(df) <- make.names(names(df), unique=TRUE)

    fname <- basename(fpath)
    low <- tolower(fname)
    ftype <- if (grepl("with.*tax", low)) "with_taxes"
             else if (grepl("without.*tax", low)) "without_taxes"
             else if (grepl("history", low)) "history"
             else "unknown"

    fallback_date <- parse_date_from_string(fname)
    dvec <- detect_date(df, fallback_date)
    if (all(is.na(dvec))) dvec <- rep(fallback_date, nrow(df))

    cidx <- detect_country(df)
    country_vec <- if (!is.na(cidx)) as.character(df[[cidx]]) else rep(NA_character_, nrow(df))

    # detect value columns → numeric-like
    value_cols <- names(df)[vapply(df, function(x) {
      if (is.numeric(x)) return(TRUE)
      y <- suppressWarnings(as.numeric(gsub(",", ".", gsub("\\s","", as.character(x)))))
      any(!is.na(y))
    }, logical(1))]

    # remove date / country columns from measure set
    drop_cols <- c("Prices.in.force.on","Date","Date.of.data","Data","Valid.on", names(df)[cidx])
    value_cols <- setdiff(value_cols, drop_cols)

    if (!length(value_cols)) return(NULL)

    for (v in value_cols) df[[v]] <- cast_numeric(df[[v]])

    # build long DF manually
    out <- vector("list", 1024L)
    k <- 0L
    for (i in seq_len(nrow(df))) {
      for (v in value_cols) {
        val <- df[[v]][i]
        if (!is.na(val)) {
          k <- k+1
          if (k > length(out)) length(out) <- k+1024L
          out[[k]] <- list(
            date          = as.Date(dvec[i]),
            country       = country_vec[i],
            product       = v,
            value         = val,
            detected_type = ftype,
            source_file   = fname
          )
        }
      }
    }
    if (k==0) return(NULL)

    out <- out[seq_len(k)]
    df_long <- do.call(rbind.data.frame, out)
    rownames(df_long) <- NULL
    df_long
  }


  # --- parse all new files ---
  parts <- vector("list", nrow(wobs_to_db))
  parsed_any <- FALSE

  for (i in seq_len(nrow(wobs_to_db))) {
    fp <- wobs_to_db$path_file[i]
    message("Parsing ", fp)
    p <- parse_one(fp)
    if (!is.null(p) && nrow(p)) {
      parts[[i]] <- p
      wobs_to_db$in_db[i] <- TRUE
      parsed_any <- TRUE
    } else {
      wobs_to_db$in_db[i] <- FALSE
    }
  }

  if (!parsed_any) {
    message("No rows parsed; updating log only.")
    logs[mask,] <- wobs_to_db
    if (file.exists(path_db_rds)) {
      full <- readRDS(path_db_rds)
      write.table(full, path_db_csv, sep=";", row.names=FALSE, col.names=TRUE)
    }
    return(logs)
  }

  db_new <- do.call(rbind, parts)

  # --- cumulative update ---
  if (!file.exists(path_db_rds)) {
    db_full <- db_new
  } else {
    old <- readRDS(path_db_rds)
    db_full <- unique(rbind(old, db_new))
  }

  saveRDS(db_full, path_db_rds)

  # always export to CSV for Power BI
  write.table(
    db_full,
    path_db_csv,
    sep = ";",
    row.names = FALSE,
    col.names = TRUE
  )

  logs[mask,] <- wobs_to_db
  logs
}


# =====================================================================
# (end of file)
# =====================================================================
