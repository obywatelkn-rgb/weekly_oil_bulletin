# =====================================================================
#  Modern Weekly Oil Bulletin Downloader (2026+)
# =====================================================================

library(xml2)
library(rvest)
library(readxl)

# =====================================================================
# PART 1 — SCRAPER
# =====================================================================

get_wob_xlsx_links <- function() {

  url_page <- "https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en"

  html <- xml2::read_html(url_page)

  hrefs <- html %>%
    html_nodes("a") %>%
    html_attr("href") %>%
    Filter(Negate(is.na), .)

  xlsx_links <- hrefs[grepl("\\.xlsx$", hrefs, ignore.case = TRUE)]

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

  keep <- grepl("price", df$file, ignore.case = TRUE) &
          !grepl("develop|dut", df$file, ignore.case = TRUE)

  df <- df[keep, , drop = FALSE]

  pattern <- "(\\d{4}[-_]\\d{2}[-_]\\d{2}|\\d{2}[-_]\\d{2}[-_]\\d{4})"
  m <- regexpr(pattern, df$file, perl = TRUE)
  dates_raw <- ifelse(m > 0, regmatches(df$file, m), NA_character_)
  dates_norm <- ifelse(is.na(dates_raw), NA_character_, gsub("_", "-", dates_raw))

  df$date <- dates_norm
  df
}

# =====================================================================
# PART 2 — LOG HANDLING
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
    message("Initialized new log.")
    return(log)
  }

  if (file.info(path_log)$size == 0) {
    message("Existing log empty; reinitializing.")
    log <- make_empty_log()
    write.table(log, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
    return(log)
  }

  log <- tryCatch(
    read.csv(path_log, stringsAsFactors = FALSE),
    error = function(e) {
      message("Log unreadable; reinitializing. ", conditionMessage(e))
      lg <- make_empty_log()
      write.table(lg, path_log, row.names = FALSE, col.names = TRUE, sep = ",")
      lg
    }
  )

  req <- c("bulletin","ind","values","url","download_date","path_file","in_db")
  if (!all(req %in% names(log))) {
    message("Log missing columns; resetting.")
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
# PART 3 — DOWNLOAD XLSX
# =====================================================================

`%nin%` <- Negate(`%in%`)

download_wobs <- function(wobs, logs, path_data = "./data/raw") {

  if (!dir.exists(path_data)) dir.create(path_data, recursive = TRUE)

  dl_mask <- wobs$url %nin% logs$url
  todo <- wobs[dl_mask, , drop = FALSE]

  message(sprintf("Found %d XLSX file(s) for download", nrow(todo)))

  if (nrow(todo) == 0) return(logs)

  for (i in seq_len(nrow(todo))) {

    url <- todo$url[i]
    file <- basename(url)
    dest <- file.path(path_data, file)

    ok <- TRUE
    tryCatch(
      download.file(url, destfile = dest, mode = "wb", quiet = TRUE),
      error = function(e) {
        message("❌ Download failed: ", conditionMessage(e))
        ok <<- FALSE
      }
    )
    if (!ok) next

    logs <- rbind(
      logs,
      data.frame(
        bulletin      = todo$date[i],
        ind           = "xlsx",
        values        = todo$date[i],
        url           = todo$url[i],
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
# PART 4 — DB BUILDER (handles ANY XLSX schema)
# =====================================================================

make_db <- function(path_dir_db, logs) {

  path_rds <- file.path(path_dir_db, "wob_full.rds")
  path_csv <- file.path(path_dir_db, "wob_full.csv")

  if (!dir.exists(path_dir_db)) dir.create(path_dir_db)

  mask <- (is.na(logs$in_db) | !logs$in_db) &
          (!is.na(logs$path_file) & nzchar(logs$path_file))

  todo <- logs[mask, , drop = FALSE]

  if (nrow(todo) == 0) {
    message("No new DB rows. Syncing CSV if needed.")
    if (file.exists(path_rds)) {
      full <- readRDS(path_rds)
      write.table(full, path_csv, sep=";", row.names=FALSE, col.names=TRUE)
    }
    return(logs)
  }

  # ---- Helper functions ----

  parse_date_token <- function(fname) {
    pat <- "(\\d{4}-\\d{2}-\\d{2}|\\d{2}-\\d{2}-\\d{4}|\\d{4}_\\d{2}_\\d{2})"
    m <- regexpr(pat, fname)
    if (m < 0) return(as.Date(NA))
    t <- gsub("_", "-", regmatches(fname, m))
    parts <- unlist(strsplit(t, "-"))
    if (nchar(parts[1]) == 4) as.Date(t)
    else as.Date(paste(parts[c(3,2,1)], collapse="-"))
  }

  detect_date_col <- function(df, fallback) {
    cn <- tolower(gsub("[^a-z0-9]+", " ", names(df)))
    idx <- which(cn %in% c("prices in force on","date","date of data","data","valid on"))
    if (!length(idx)) return(rep(fallback, nrow(df)))
    raw <- df[[idx[1]]]
    suppressWarnings(
      x <- as.Date(raw, tryFormats=c("%Y-%m-%d","%d-%m-%Y","%d/%m/%Y","%Y/%m/%d"))
    )
    x[is.na(x)] <- fallback
    x
  }

  detect_country_col <- function(df) {
    cn <- tolower(gsub("[^a-z0-9]+", " ", names(df)))
    hit <- grep("\\bcountry\\b|member state|geo|^ms$", cn)
    if (length(hit)) hit[1] else NA_integer_
  }

  cast_numeric <- function(x) {
    if (is.numeric(x)) return(x)
    suppressWarnings(as.numeric(gsub(",", ".", gsub("\\s","", as.character(x)))))
  }

  parse_one <- function(fp) {
    if (!file.exists(fp)) return(NULL)

    df <- tryCatch(
      read_excel(fp, col_names = TRUE, na = c("N.A","N/A")),
      error = function(e) {
        message("❌ XLSX read failed: ", conditionMessage(e))
        return(NULL)
      }
    )
    if (is.null(df) || !nrow(df)) return(NULL)

    names(df) <- make.names(names(df), unique = TRUE)

    fname <- basename(fp)
    low   <- tolower(fname)

    ftype <- if (grepl("with.*tax", low)) "with_taxes"
             else if (grepl("without.*tax", low)) "without_taxes"
             else if (grepl("history", low)) "history"
             else "unknown"

    fb_date <- parse_date_token(fname)
    dvec <- detect_date_col(df, fb_date)
    if (all(is.na(dvec))) dvec <- rep(fb_date, nrow(df))

    cidx <- detect_country_col(df)
    country_vec <- if (!is.na(cidx)) as.character(df[[cidx]]) else rep(NA_character_, nrow(df))

    value_cols <- names(df)[
      vapply(df, function(x) {
        if (is.numeric(x)) TRUE else {
          y <- suppressWarnings(as.numeric(gsub(",", ".", gsub("\\s","", as.character(x)))))
          any(!is.na(y))
        }
      }, logical(1))
    ]

    drop <- c("Prices.in.force.on","Date","Date.of.data","Data","Valid.on", names(df)[cidx])
    value_cols <- setdiff(value_cols, drop)
    if (!length(value_cols)) return(NULL)

    for (v in value_cols) df[[v]] <- cast_numeric(df[[v]])

    out <- vector("list", 4096L)
    k <- 0L
    for (i in seq_len(nrow(df))) {
      for (v in value_cols) {
        val <- df[[v]][i]
        if (!is.na(val)) {
          k <- k+1
          if (k > length(out)) length(out) <- k+4096L
          out[[k]] <- list(
            date = as.Date(dvec[i]),
            country = country_vec[i],
            product = v,
            value = val,
            detected_type = ftype,
            source_file = fname
          )
        }
      }
    }
    if (k == 0) return(NULL)
    out <- out[seq_len(k)]
    do.call(rbind.data.frame, out)
  }

  # ---- Parse all new files ----

  parts <- vector("list", nrow(todo))
  parsed_any <- FALSE

  for (i in seq_len(nrow(todo))) {
    fp <- todo$path_file[i]
    message("Parsing ", fp)
    df_long <- parse_one(fp)
    if (!is.null(df_long) && nrow(df_long)) {
      parts[[i]] <- df_long
      todo$in_db[i] <- TRUE
      parsed_any <- TRUE
    } else {
      todo$in_db[i] <- FALSE
    }
  }

  if (!parsed_any) {
    message("No rows parsed.")
    logs[mask,] <- todo
    return(logs)
  }

  db_new <- do.call(rbind, parts)

  if (!file.exists(path_rds)) {
    db_full <- db_new
  } else {
    old <- readRDS(path_rds)
    db_full <- unique(rbind(old, db_new))
  }

  saveRDS(db_full, path_rds)

  write.table(
    db_full,
    path_csv,
    sep = ";",
    row.names = FALSE,
    col.names = TRUE
  )

  logs[mask,] <- todo
  logs
}

# =====================================================================
