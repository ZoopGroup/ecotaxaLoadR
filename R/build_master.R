#' @title Build master tow summary
#'
#' @description
#' Summarize PRO-derived cast data by tow and net, then append HEX header
#' metadata to produce a master table for downstream analyses.
#'
#' @param pro_data_file Data frame produced by [load_pro_files()]. Should
#'   include `vessel`, `cruise_id`, `tow_number`, `net`, `lat`, `lon` (or
#'   `long`), `CTDDEPTH(M)`, `vol`, `datetime_gmt`, `datetime_local`, and
#'   optionally `is_day`.
#' @param hex_data_file Data frame produced by [load_hex_files()] containing at
#'   minimum `tow` and `net_size` columns; if `net` is present, joining is
#'   performed on both `tow` and `net`.
#'
#' @return A tibble with one row per tow-number and net combination containing:
#'   `vessel`, `cruise`, `tow`, `net`, `lat_decimal`, `long_decimal`,
#'   `min_depth`, `max_depth`, `volume_filtered`, `time_start_gmt`,
#'   `date_start_gmt`,
#'   `time_start_local`, `is_day`, and `net_size`.
#'
#' @details
#' Warnings are emitted once per call when:
#' - `is_day` is absent (the column is added and filled with `NA`).
#' - Multiple distinct `is_day` values exist within a tow/net group (the first
#'   value is kept).
#' - Non-numeric `tow`/`net` key values are encountered while coercing keys to
#'   integer for joins.
#'
#' @examples
#' \dontrun{
#' pro_data <- ecotaxaLoadR::load_pro_files(
#'   directory = "path/to/pro/",
#'   daynight  = TRUE
#' )
#'
#' hex_data <- ecotaxaLoadR::load_hex_files(
#'   directory = "path/to/hex/"
#' )
#'
#' master <- ecotaxaLoadR::build_master(
#'   pro_data_file = pro_data,
#'   hex_data_file = hex_data
#' )
#' }
#'
#' @importFrom dplyr group_by summarise ungroup mutate select left_join rename
#' @importFrom dplyr n_distinct first
#' @importFrom rlang .data
#' @export
build_master <- function(pro_data_file, hex_data_file) {
  if (!is.data.frame(pro_data_file)) {
    stop("`pro_data_file` must be a data frame produced by load_pro_files().")
  }

  if (!is.data.frame(hex_data_file)) {
    stop("`hex_data_file` must be a data frame produced by load_hex_files().")
  }

  required_pro_cols <- c(
    "vessel", "cruise_id", "tow_number", "net", "lat",
    "CTDDEPTH(M)", "vol", "datetime_gmt", "datetime_local"
  )

  missing_pro <- setdiff(required_pro_cols, names(pro_data_file))
  if (length(missing_pro) > 0) {
    stop(
      "Missing required columns in pro_data_file: ",
      paste(missing_pro, collapse = ", ")
    )
  }

  # longitude column may appear as either `long` or `lon`
  long_col <-
    if ("long" %in% names(pro_data_file)) {
      "long"
    } else if ("lon" %in% names(pro_data_file)) {
      "lon"
    } else {
      stop("`pro_data_file` must contain either a `long` or `lon` column.")
    }

  if (!all(c("tow", "net_size") %in% names(hex_data_file))) {
    stop("`hex_data_file` must contain `tow` and `net_size` columns.")
  }

  # add is_day if absent and warn once
  has_is_day <- "is_day" %in% names(pro_data_file)
  if (!has_is_day) {
    warning("`is_day` column missing; output will contain NA for `is_day`.")
    pro_data_file$is_day <- NA
  }

  # warn once when multiple distinct is_day values occur within tow/net
  is_day_conflicts <- pro_data_file |>
    dplyr::group_by(.data$tow_number, .data$net) |>
    dplyr::summarise(
      distinct_is_day = dplyr::n_distinct(.data$is_day, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$distinct_is_day > 1)

  if (nrow(is_day_conflicts) > 0) {
    examples <- utils::head(
      paste0(
        "tow ", is_day_conflicts$tow_number,
        " net ", is_day_conflicts$net
      ),
      5
    )

    warning(
      "Multiple `is_day` values detected within tow/net; using the first value. ",
      "Examples: ",
      paste(examples, collapse = "; ")
    )
  }

  safe_min <- function(x) {
    if (all(is.na(x))) {
      return(x[NA_integer_])
    }
    suppressWarnings(min(x, na.rm = TRUE))
  }

  safe_max <- function(x) {
    if (all(is.na(x))) {
      return(x[NA_integer_])
    }
    suppressWarnings(max(x, na.rm = TRUE))
  }

  coerce_integer_key <- function(x, key_name, source_name) {
    x_chr <- trimws(as.character(x))
    x_chr[x_chr == ""] <- NA_character_
    x_num <- suppressWarnings(as.numeric(x_chr))

    bad_rows <- which(!is.na(x_chr) & is.na(x_num))
    if (length(bad_rows) > 0) {
      examples <- utils::head(unique(x_chr[bad_rows]), 5)
      warning(
        "Non-numeric values detected in ", source_name, "$", key_name,
        "; coerced to NA. Examples: ",
        paste(examples, collapse = ", ")
      )
    }

    as.integer(x_num)
  }

  pro_summary <- pro_data_file |>
    dplyr::group_by(.data$tow_number, .data$net) |>
    dplyr::summarise(
      vessel          = dplyr::first(.data$vessel),
      cruise          = dplyr::first(.data$cruise_id),
      lat_decimal     = safe_min(.data$lat),
      long_decimal    = safe_min(.data[[long_col]]),
      min_depth       = safe_min(.data[["CTDDEPTH(M)"]]),
      max_depth       = safe_max(.data[["CTDDEPTH(M)"]]),
      volume_filtered = safe_max(.data$vol),
      time_start_gmt  = safe_min(.data$datetime_gmt),
      date_start_gmt  = base::as.Date(safe_min(.data$datetime_gmt)),
      time_start_local = safe_min(.data$datetime_local),
      is_day          = dplyr::first(.data$is_day),
      .groups = "drop"
    ) |>
    dplyr::rename(tow = tow_number) |>
    dplyr::mutate(
      tow = coerce_integer_key(.data$tow, "tow", "pro_data_file"),
      net = coerce_integer_key(.data$net, "net", "pro_data_file")
    )

  hex_ready <- hex_data_file |>
    dplyr::mutate(
      tow = coerce_integer_key(.data$tow, "tow", "hex_data_file")
    )

  by_keys <- "tow"
  if ("net" %in% names(hex_ready)) {
    hex_ready <- hex_ready |>
      dplyr::mutate(net = coerce_integer_key(.data$net, "net", "hex_data_file"))
    by_keys <- c("tow", "net")
  }

  hex_ready <- hex_ready |>
    dplyr::select(dplyr::all_of(by_keys), "net_size")

  master <- pro_summary |>
    dplyr::left_join(hex_ready, by = by_keys) |>
    dplyr::select(
      vessel, cruise, tow, net,
      lat_decimal, long_decimal,
      min_depth, max_depth,
      volume_filtered,
      time_start_gmt, date_start_gmt, time_start_local,
      is_day, net_size
    )

  return(master)
}
